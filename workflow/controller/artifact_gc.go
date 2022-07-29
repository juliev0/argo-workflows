package controller

import (
	"context"
	"fmt"
	"hash/fnv"
	"sort"

	"golang.org/x/exp/maps"
	apiv1 "k8s.io/api/core/v1"
	corev1 "k8s.io/api/core/v1"
	apierr "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/env"
	"k8s.io/utils/pointer"

	"github.com/argoproj/argo-workflows/v3/pkg/apis/workflow"
	wfv1 "github.com/argoproj/argo-workflows/v3/pkg/apis/workflow/v1alpha1"
	"github.com/argoproj/argo-workflows/v3/util/slice"
	"github.com/argoproj/argo-workflows/v3/workflow/common"
	"github.com/argoproj/argo-workflows/v3/workflow/controller/indexes"
)

const artifactGCComponent = "artifact-gc"

// artifactGCEnabled is a feature flag to globally disabled artifact GC in case of emergency
var artifactGCEnabled, _ = env.GetBool("ARGO_ARTIFACT_GC_ENABLED", true)

func (woc *wfOperationCtx) garbageCollectArtifacts(ctx context.Context) error {

	if !artifactGCEnabled {
		return nil
	}
	// only do Artifact GC if we have a Finalizer for it (i.e. Artifact GC is configured for this Workflow
	// and there's work left to do for it)
	if !slice.ContainsString(woc.wf.Finalizers, common.FinalizerArtifactGC) {
		if woc.execWf.HasArtifactGC() { //todo: if this works, consider adding something to status to prevent re-checking
			woc.log.Info("adding artifact GC finalizer")
			finalizers := append(woc.wf.GetFinalizers(), common.FinalizerArtifactGC)
			woc.wf.SetFinalizers(finalizers)
		}
		return nil
	}

	if woc.wf.Status.ArtifactGCStatus == nil {
		woc.wf.Status.ArtifactGCStatus = &wfv1.ArtGCStatus{}
	}

	// based on current state of Workflow, which Artifact GC Strategies can be processed now?
	strategies := woc.artifactGCStrategiesReady()
	for strategy, _ := range strategies {
		woc.log.Debugf("processing Artifact GC Strategy %s", strategy)
		err := woc.processArtifactGCStrategy(ctx, strategy)
		if err != nil {
			return err
		}
	}

	err := woc.processArtifactGCCompletion(ctx)
	if err != nil {
		return err
	}
	return nil
}

// which ArtifactGC Strategies are ready to process?
func (woc *wfOperationCtx) artifactGCStrategiesReady() map[wfv1.ArtifactGCStrategy]struct{} {
	strategies := map[wfv1.ArtifactGCStrategy]struct{}{} // essentially a Set

	if woc.wf.Labels[common.LabelKeyCompleted] == "true" || woc.wf.DeletionTimestamp != nil {
		if !woc.wf.Status.ArtifactGCStatus.IsArtifactGCStrategyProcessed(wfv1.ArtifactGCOnWorkflowCompletion) {
			strategies[wfv1.ArtifactGCOnWorkflowCompletion] = struct{}{}
		}
	}
	if woc.wf.DeletionTimestamp != nil {
		if !woc.wf.Status.ArtifactGCStatus.IsArtifactGCStrategyProcessed(wfv1.ArtifactGCOnWorkflowDeletion) {
			strategies[wfv1.ArtifactGCOnWorkflowDeletion] = struct{}{}
		}
	}
	if woc.wf.Status.Successful() {
		if !woc.wf.Status.ArtifactGCStatus.IsArtifactGCStrategyProcessed(wfv1.ArtifactGCOnWorkflowSuccess) {
			strategies[wfv1.ArtifactGCOnWorkflowSuccess] = struct{}{}
		}
	}
	if woc.wf.Status.Failed() {
		if !woc.wf.Status.ArtifactGCStatus.IsArtifactGCStrategyProcessed(wfv1.ArtifactGCOnWorkflowFailure) {
			strategies[wfv1.ArtifactGCOnWorkflowFailure] = struct{}{}
		}
	}

	return strategies
}

type templatesToArtifacts map[string]wfv1.ArtifactSearchResults

// Artifact GC Strategy is ready: start up Pods to handle it
func (woc *wfOperationCtx) processArtifactGCStrategy(ctx context.Context, strategy wfv1.ArtifactGCStrategy) error {

	defer func() {
		woc.wf.Status.ArtifactGCStatus.SetArtifactGCStrategyProcessed(strategy, true)
		woc.updated = true
	}()

	var err error

	woc.log.Debugf("processing Artifact GC Strategy %s", strategy)

	// Search for artifacts // todo: execWf or wf?
	//artifactSearchResults := woc.execWf.SearchArtifacts(&wfv1.ArtifactSearchQuery{ArtifactGCStrategies: map[wfv1.ArtifactGCStrategy]bool{strategy: true}, Deleted: pointer.BoolPtr(false)})
	artifactSearchResults := woc.wf.SearchArtifacts(&wfv1.ArtifactSearchQuery{ArtifactGCStrategies: map[wfv1.ArtifactGCStrategy]bool{strategy: true}, Deleted: pointer.BoolPtr(false)})
	if len(artifactSearchResults) == 0 {
		woc.log.Debugf("No Artifact Search Results returned from strategy %s", strategy)
		return nil
	}

	// cache the templates by name so we can find them easily
	templatesByName := make(map[string]*wfv1.Template)

	/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	// We need to create a separate Pod for each set of Artifacts that require special permissions
	// (i.e. Service Account and Pod Metadata)
	// So first group artifacts that need to be deleted by permissions
	/////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	groupedByPod := make(map[string]templatesToArtifacts)

	// a mapping from the name we'll use for the Pod to the actual metadata and Service Account that need to be applied for that Pod
	podNames := make(map[string]podInfo)

	var podName string
	var podAccessInfo podInfo

	for _, artifactSearchResult := range artifactSearchResults {
		// get the permissions required for this artifact and create a unique Pod name from them
		podAccessInfo = woc.getArtifactGCPodInfo(&artifactSearchResult.Artifact)
		podName = woc.artGCPodName(strategy, podAccessInfo)
		_, found := podNames[podName]
		if !found {
			podNames[podName] = podAccessInfo
		}
		_, found = groupedByPod[podName]
		if !found {
			groupedByPod[podName] = make(templatesToArtifacts)
		}
		// get the Template for the Artifact
		node, found := woc.wf.Status.Nodes[artifactSearchResult.NodeID]
		if !found {
			return fmt.Errorf("can't process Artifact GC Strategy %s: node ID '%s' not found in Status??", strategy, artifactSearchResult.NodeID)
		}
		templateName := node.TemplateName
		if templateName == "" && node.GetTemplateRef() != nil {
			templateName = node.GetTemplateRef().Name
		}
		if templateName == "" {
			return fmt.Errorf("can't process Artifact GC Strategy %s: node %+v has an unnamed template", strategy, node)
		}
		template, found := templatesByName[templateName]
		if !found {
			template = woc.wf.GetTemplateByName(templateName)
			if template == nil {
				return fmt.Errorf("can't process Artifact GC Strategy %s: template name '%s' belonging to node %+v not found??", strategy, node.TemplateName, node)
			}
			templatesByName[templateName] = template
		}

		_, found = groupedByPod[podName][template.Name]
		if !found {
			groupedByPod[podName][template.Name] = make(wfv1.ArtifactSearchResults, 0)
		}

		groupedByPod[podName][template.Name] = append(groupedByPod[podName][template.Name], artifactSearchResult)
	}

	fmt.Printf("deletethis: groupedByPod=%+v\n", groupedByPod)

	// start up a separate Pod with a separate set of ArtifactGCTasks for it to use for each unique Service Account/metadata
	for podName, templatesToArtList := range groupedByPod {
		tasks := make([]*wfv1.WorkflowArtifactGCTask, 0)

		fmt.Printf("deletethis: processing podName %s from groupedByPod\n", podName)

		for templateName, artifacts := range templatesToArtList {
			fmt.Printf("deletethis: for podName '%s' from groupedByPod, processing templateName '%s'\n", podName, templateName)
			template := templatesByName[templateName]
			woc.addTemplateArtifactsToTasks(strategy, podName, &tasks, template, artifacts)
		}

		if len(tasks) > 0 {
			// create the K8s WorkflowArtifactGCTask objects
			for i, task := range tasks {
				tasks[i], err = woc.createWorkflowArtifactGCTask(ctx, task)
				if err != nil {
					return err
				}
			}
			// create the pod
			podAccessInfo, found := podNames[podName]
			if !found {
				return fmt.Errorf("can't find podInfo for podName '%s'??", podName)
			}
			_, err := woc.createArtifactGCPod(ctx, strategy, tasks, podAccessInfo, podName)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

type podInfo struct {
	serviceAccount string
	podMetadata    wfv1.Metadata
}

// get Pod name
// (we have a unique Pod for each Artifact GC Strategy and Service Account/Metadata requirement)
func (woc *wfOperationCtx) artGCPodName(strategy wfv1.ArtifactGCStrategy, podAccessInfo podInfo) string {
	h := fnv.New32a()
	_, _ = h.Write([]byte(podAccessInfo.serviceAccount))
	// we should be able to always get the same result regardless of the order of our Labels or Annotations
	// so sort alphabetically
	sortedLabels := maps.Keys(podAccessInfo.podMetadata.Labels)
	sort.Strings(sortedLabels)
	for _, label := range sortedLabels {
		labelValue := podAccessInfo.podMetadata.Labels[label]
		_, _ = h.Write([]byte(label))
		_, _ = h.Write([]byte(labelValue))
	}

	sortedAnnotations := maps.Keys(podAccessInfo.podMetadata.Annotations)
	sort.Strings(sortedAnnotations)
	for _, annotation := range sortedAnnotations {
		annotationValue := podAccessInfo.podMetadata.Annotations[annotation]
		_, _ = h.Write([]byte(annotation))
		_, _ = h.Write([]byte(annotationValue))
	}

	return fmt.Sprintf("%s-artgc-%s-%v", woc.wf.Name, strategy.AbbreviatedName(), h.Sum32())
}

func (woc *wfOperationCtx) artGCTaskName(podName string, taskIndex int) string {
	return fmt.Sprintf("%s-%d", podName, taskIndex)
}

func (woc *wfOperationCtx) addTemplateArtifactsToTasks(strategy wfv1.ArtifactGCStrategy, podName string, tasks *[]*wfv1.WorkflowArtifactGCTask, template *wfv1.Template, artifactSearchResults wfv1.ArtifactSearchResults) {
	if len(artifactSearchResults) == 0 {
		return
	}
	if tasks == nil {
		ts := make([]*wfv1.WorkflowArtifactGCTask, 0)
		tasks = &ts
	}

	// do we need to generate a new WorkflowArtifactGCTask or can we use current?
	// todo: currently we're only handling one but may require more in the future if we start to reach 1 MB in the CRD
	if len(*tasks) == 0 {
		currentTask := &wfv1.WorkflowArtifactGCTask{
			TypeMeta: metav1.TypeMeta{
				Kind:       workflow.WorkflowArtifactGCTaskKind,
				APIVersion: workflow.APIVersion,
			},
			ObjectMeta: metav1.ObjectMeta{
				Namespace: woc.wf.Namespace,
				Name:      woc.artGCTaskName(podName, 0),
				Labels:    map[string]string{common.LabelKeyArtifactGCPodName: podName},
				OwnerReferences: []metav1.OwnerReference{ // make sure we get deleted with the workflow
					*metav1.NewControllerRef(woc.wf, wfv1.SchemeGroupVersion.WithKind(workflow.WorkflowKind)),
				},
			},
			Spec: wfv1.ArtifactGCSpec{
				ArtifactsByNode: make(map[string]wfv1.ArtifactNodeSpec),
			},
		}
		*tasks = append(*tasks, currentTask)
	} /*else if hitting 1 MB on CRD { //todo: handle multiple WorkflowArtifactGCTasks
		// add a new WorkflowArtifactGCTask to *tasks
	}*/

	currentTask := (*tasks)[len(*tasks)-1]
	artifactsByNode := currentTask.Spec.ArtifactsByNode

	// if ArchiveLocation is specified for the Template use that, otherwise use default
	archiveLocation := template.ArchiveLocation
	if archiveLocation == nil {
		archiveLocation = woc.artifactRepository.ToArtifactLocation()
	}

	// go through artifactSearchResults and create a map from nodeID to artifacts
	// for each node, create an ArtifactNodeSpec with our Template's ArchiveLocation (if any) and our list of Artifacts
	for _, artifactSearchResult := range artifactSearchResults {
		artifactNodeSpec, found := artifactsByNode[artifactSearchResult.NodeID]
		if !found {
			artifactsByNode[artifactSearchResult.NodeID] = wfv1.ArtifactNodeSpec{
				ArchiveLocation: archiveLocation,
				Artifacts:       make(map[string]wfv1.Artifact),
			}
			artifactNodeSpec = artifactsByNode[artifactSearchResult.NodeID]
		}

		artifactNodeSpec.Artifacts[artifactSearchResult.Name] = artifactSearchResult.Artifact

	}
	woc.log.Debugf("list of artifacts pertaining to template %s to WorkflowArtifactGCTask '%s': %+v", template.Name, currentTask.Name, artifactsByNode)

}

// find WorkflowArtifactGCTask CRD object by name
func (woc *wfOperationCtx) getArtifactTask(taskName string) (*wfv1.WorkflowArtifactGCTask, error) {
	key := woc.wf.Namespace + "/" + taskName
	task, exists, err := woc.controller.artGCTaskInformer.Informer().GetIndexer().GetByKey(key)
	if err != nil {
		return nil, fmt.Errorf("failed to get WorkflowArtifactGCTask by key '%s': %w", key, err)
	}
	if !exists {
		return nil, nil
	}
	return task.(*wfv1.WorkflowArtifactGCTask), nil
}

//	create WorkflowArtifactGCTask CRD object
func (woc *wfOperationCtx) createWorkflowArtifactGCTask(ctx context.Context, task *wfv1.WorkflowArtifactGCTask) (*wfv1.WorkflowArtifactGCTask, error) {

	// first make sure it doesn't already exist
	foundTask, err := woc.getArtifactTask(task.Name)
	if err != nil {
		return nil, err
	}
	if foundTask != nil {
		woc.log.Debugf("Artifact GC Task %s already exists", task.Name)
	} else {
		woc.log.Infof("Creating Artifact GC Task %s", task.Name)

		task, err = woc.controller.wfclientset.ArgoprojV1alpha1().WorkflowArtifactGCTasks(woc.wf.Namespace).Create(ctx, task, metav1.CreateOptions{})
		if err != nil {
			return nil, fmt.Errorf("failed to Create WorkflowArtifactGCTask '%s' for Garbage Collection: %w", task.Name, err)
		}
	}
	return task, nil
}

// create the Pod which will do the deletions
func (woc *wfOperationCtx) createArtifactGCPod(ctx context.Context, strategy wfv1.ArtifactGCStrategy, tasks []*wfv1.WorkflowArtifactGCTask,
	podAccessInfo podInfo, podName string) (*corev1.Pod, error) {

	woc.log.
		WithField("strategy", strategy).
		Infof("creating pod to delete artifacts: %s", podName)

	// Pod is owned by WorkflowArtifactGCTasks, so it will die automatically when all of them have died
	ownerReferences := make([]metav1.OwnerReference, len(tasks))
	for i, task := range tasks {
		// make sure pod gets deleted with the WorkflowArtifactGCTasks
		ownerReferences[i] = *metav1.NewControllerRef(task, wfv1.SchemeGroupVersion.WithKind(workflow.WorkflowArtifactGCTaskKind))
	}

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: podName,
			Labels: map[string]string{
				common.LabelKeyWorkflow:  woc.wf.Name,
				common.LabelKeyComponent: artifactGCComponent,
				common.LabelKeyCompleted: "false",
			},
			Annotations: map[string]string{
				common.AnnotationKeyArtifactGCStrategy: string(strategy),
			},

			OwnerReferences: ownerReferences,
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:            common.MainContainerName,
					Image:           woc.controller.executorImage(),
					ImagePullPolicy: woc.controller.executorImagePullPolicy(),
					Args:            []string{"artifact", "delete", "--loglevel", getExecutorLogLevel()},
					Env: []corev1.EnvVar{
						{Name: common.EnvVarArtifactGCPod, Value: podName},
					},
					// if this pod is breached by an attacker we:
					// * prevent installation of any new packages
					// * modification of the file-system
					SecurityContext: &corev1.SecurityContext{
						Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
						Privileged:               pointer.Bool(false),
						RunAsNonRoot:             pointer.Bool(true),
						RunAsUser:                pointer.Int64Ptr(8737), //todo: magic number
						ReadOnlyRootFilesystem:   pointer.Bool(true),
						AllowPrivilegeEscalation: pointer.Bool(false),
					},
					// if this pod is breached by an attacker these limits prevent excessive CPU and memory usage
					Resources: corev1.ResourceRequirements{
						Limits: map[corev1.ResourceName]resource.Quantity{
							"cpu":    resource.MustParse("100m"), //todo: should these values be in the Controller config, and also maybe increased?
							"memory": resource.MustParse("64Mi"),
						},
						Requests: map[corev1.ResourceName]resource.Quantity{
							"cpu":    resource.MustParse("50m"),
							"memory": resource.MustParse("32Mi"),
						},
					},
				},
			},
			AutomountServiceAccountToken: pointer.Bool(true),
			RestartPolicy:                corev1.RestartPolicyNever,
		},
	}

	// Use the Service Account and/or Labels and Annotations specified for our Pod, if they exist
	if podAccessInfo.serviceAccount != "" {
		pod.Spec.ServiceAccountName = podAccessInfo.serviceAccount
	}
	for label, labelVal := range podAccessInfo.podMetadata.Labels {
		pod.ObjectMeta.Labels[label] = labelVal
	}
	for annotation, annotationVal := range podAccessInfo.podMetadata.Annotations {
		pod.ObjectMeta.Annotations[annotation] = annotationVal
	}

	if v := woc.controller.Config.InstanceID; v != "" {
		pod.Labels[common.EnvVarInstanceID] = v
	}

	_, err := woc.controller.kubeclientset.CoreV1().Pods(woc.wf.Namespace).Create(ctx, pod, metav1.CreateOptions{})

	if err != nil {
		if apierr.IsAlreadyExists(err) {
			woc.log.Warningf("Artifact GC Pod %s already exists?", pod.Name)
		} else {
			return nil, fmt.Errorf("failed to create pod: %w", err)
		}
	}
	return pod, nil
}

// go through any GC pods that are already running and may have completed
func (woc *wfOperationCtx) processArtifactGCCompletion(ctx context.Context) error {
	// check if any previous Artifact GC Pods completed
	pods, err := woc.controller.podInformer.GetIndexer().ByIndex(indexes.WorkflowIndex, woc.wf.GetNamespace()+"/"+woc.wf.GetName())
	if err != nil {
		return fmt.Errorf("failed to get pods from informer: %w", err)
	}

	anyPodSuccess := false
	for _, obj := range pods {
		pod := obj.(*corev1.Pod)
		if pod.Labels[common.LabelKeyComponent] != artifactGCComponent { // make sure it's an Artifact GC Pod
			continue
		}

		// make sure we didn't already process this one
		if woc.wf.Status.ArtifactGCStatus.IsArtifactGCPodRecouped(pod.Name) {
			// already processed
			fmt.Printf("deletethis: pod %s was already recouped\n", pod.Name)
			continue
		}

		phase := pod.Status.Phase
		fmt.Printf("deletethis: notification for status change of pod %s, phase: %v\n", pod.Name, phase)

		// if Pod is done process the results
		if phase == corev1.PodSucceeded || phase == corev1.PodFailed {
			woc.log.WithField("pod", pod.Name).
				WithField("phase", phase).
				WithField("message", pod.Status.Message).
				Info("reconciling artifact-gc pod")

			err = woc.processCompletedArtifactGCPod(ctx, pod)
			if err != nil {
				return err
			}
			woc.wf.Status.ArtifactGCStatus.SetArtifactGCPodRecouped(pod.Name, true)
			if phase == corev1.PodSucceeded {
				anyPodSuccess = true
			}
			woc.updated = true
		}
	}

	if anyPodSuccess {
		// check if all artifacts have been deleted and if so remove Finalizer
		if woc.allArtifactsDeleted() {
			woc.log.Info("no remaining artifacts to GC, removing artifact GC finalizer")
			woc.wf.Finalizers = slice.RemoveString(woc.wf.Finalizers, common.FinalizerArtifactGC)
			woc.updated = true
		}

	}
	return nil
}

func (woc *wfOperationCtx) allArtifactsDeleted() bool {
	for _, n := range woc.wf.Status.Nodes {
		for _, a := range n.GetOutputs().GetArtifacts() {
			if !a.Deleted && a.GetArtifactGC().Strategy != wfv1.ArtifactGCNever {
				return false
			}
		}
	}
	return true
}

func (woc *wfOperationCtx) processCompletedArtifactGCPod(ctx context.Context, pod *corev1.Pod) error {
	woc.log.Infof("processing completed Artifact GC Pod '%s'", pod.Name)

	// get associated WorkflowArtifactGCTasks
	labelSelector := fmt.Sprintf("%s = %s", common.LabelKeyArtifactGCPodName, pod.Name)
	taskList, err := woc.controller.wfclientset.ArgoprojV1alpha1().WorkflowArtifactGCTasks(woc.wf.Namespace).List(ctx, metav1.ListOptions{LabelSelector: labelSelector})
	if err != nil {
		return fmt.Errorf("failed to List WorkflowArtifactGCTasks: %w", err)
	}

	strategyStr, found := pod.Annotations[common.AnnotationKeyArtifactGCStrategy]
	if !found {
		return fmt.Errorf("Artifact GC Pod '%s' doesn't have annotation '%s'?", pod.Name, common.AnnotationKeyArtifactGCStrategy)
	}
	fmt.Printf("deletethis: processing pod %s for strategy %s\n", pod.Name, strategyStr)
	strategy := wfv1.ArtifactGCStrategy(strategyStr)

	for _, task := range taskList.Items {
		err = woc.processCompletedWorkflowArtifactGCTask(ctx, &task, strategy)
		if err != nil {
			return err
		}
	}
	return nil
}

// process the Status in the WorkflowArtifactGCTask which was completed and reflect it in Workflow Status; then delete the Task CRD Object
// return first found error message if GC failed
func (woc *wfOperationCtx) processCompletedWorkflowArtifactGCTask(ctx context.Context, artifactGCTask *wfv1.WorkflowArtifactGCTask, strategy wfv1.ArtifactGCStrategy) error {
	woc.log.Debugf("processing WorkflowArtifactGCTask %s", artifactGCTask.Name)

	foundGCFailure := false
	for nodeName, nodeResult := range artifactGCTask.Status.ArtifactResultsByNode {
		// find this node result in the Workflow Status
		wfNode, found := woc.wf.Status.Nodes[nodeName]
		if !found {
			return fmt.Errorf("node named '%s' returned by WorkflowArtifactGCTask '%s' wasn't found in Workflow '%s' Status", nodeName, artifactGCTask.Name, woc.wf.Name)
		}
		if wfNode.Outputs == nil {
			return fmt.Errorf("node named '%s' returned by WorkflowArtifactGCTask '%s' doesn't seem to have Outputs in Workflow Status", nodeName, artifactGCTask.Name)
		}
		for i, wfArtifact := range wfNode.Outputs.Artifacts {
			// find artifact in the WorkflowArtifactGCTask Status
			artifactResult, foundArt := nodeResult.ArtifactResults[wfArtifact.Name]
			if !foundArt {
				// could be in a different WorkflowArtifactGCTask
				continue
			}
			fmt.Printf("deletethis: setting artifact Deleted=%t, %+v\n", artifactResult.Success, woc.wf.Status.Nodes[nodeName].Outputs.Artifacts[i])
			woc.wf.Status.Nodes[nodeName].Outputs.Artifacts[i].Deleted = artifactResult.Success
			if artifactResult.Error != nil {
				// issue an Event if there was an error - just do this one to prevent flooding the system with Events
				if !foundGCFailure {
					foundGCFailure = true
					gcFailureMsg := *artifactResult.Error
					woc.eventRecorder.Event(woc.wf, apiv1.EventTypeWarning, "ArtifactGCFailed",
						fmt.Sprintf("Artifact Garbage Collection failed for strategy %s, err:%s", strategy, gcFailureMsg))
				}
			}
		}

	}

	// now we can delete it, if it succeeded (otherwise we leave it up to be inspected)
	if !foundGCFailure {
		// todo: temporarily commented out for testing
		//woc.log.Debugf("deleting WorkflowArtifactGCTask: %s", artifactGCTask.Name)
		//woc.controller.wfclientset.ArgoprojV1alpha1().WorkflowArtifactGCTasks(woc.wf.Namespace).Delete(ctx, artifactGCTask.Name, metav1.DeleteOptions{})
	}
	return nil
}

func (woc *wfOperationCtx) getArtifactGCPodInfo(artifact *wfv1.Artifact) podInfo {
	//  start with Workflow.ArtifactGC and override with Artifact.ArtifactGC
	podAccessInfo := podInfo{}
	if woc.wf.Spec.ArtifactGC != nil {
		woc.updateArtifactGCPodInfo(woc.wf.Spec.ArtifactGC, &podAccessInfo)
	}
	if artifact.ArtifactGC != nil {
		woc.updateArtifactGCPodInfo(artifact.ArtifactGC, &podAccessInfo)
	}
	return podAccessInfo
}

// propagate the information from artifactGC into the podInfo
func (woc *wfOperationCtx) updateArtifactGCPodInfo(artifactGC *wfv1.ArtifactGC, podAccessInfo *podInfo) {

	if artifactGC.ServiceAccountName != "" {
		podAccessInfo.serviceAccount = artifactGC.ServiceAccountName
	}
	if artifactGC.PodMetadata != nil {
		if len(artifactGC.PodMetadata.Labels) > 0 && podAccessInfo.podMetadata.Labels == nil {
			podAccessInfo.podMetadata.Labels = make(map[string]string)
		}
		for labelKey, labelValue := range artifactGC.PodMetadata.Labels {
			podAccessInfo.podMetadata.Labels[labelKey] = labelValue
		}
		if len(artifactGC.PodMetadata.Annotations) > 0 && podAccessInfo.podMetadata.Annotations == nil {
			podAccessInfo.podMetadata.Annotations = make(map[string]string)
		}
		for annotationKey, annotationValue := range artifactGC.PodMetadata.Annotations {
			podAccessInfo.podMetadata.Annotations[annotationKey] = annotationValue
		}
	}

}