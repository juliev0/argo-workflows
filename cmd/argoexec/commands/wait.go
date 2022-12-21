package commands

import (
	"context"
	"fmt"
	"os/signal"
	"syscall"
	"time"

	"github.com/argoproj/pkg/stats"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

func NewWaitCommand() *cobra.Command {
	command := cobra.Command{
		Use:   "wait",
		Short: "wait for main container to finish and save artifacts",
		Run: func(cmd *cobra.Command, args []string) {
			ctx := context.Background()
			err := waitContainer(ctx)
			if err != nil {
				log.Fatalf("%+v", err)
			}
		},
	}
	return &command
}

func waitContainer(ctx context.Context) error {
	wfExecutor := initExecutor()
	defer wfExecutor.HandleError(ctx) // Must be placed at the bottom of defers stack.
	defer stats.LogStats()
	stats.StartStatsTicker(5 * time.Minute)

	fmt.Println("wait iteration 1")

	// use a function to constrain the scope of ctx
	func() {
		// this allows us to gracefully shutdown, capturing artifacts
		ctx, cancel := signal.NotifyContext(ctx, syscall.SIGTERM)
		defer cancel()

		// Wait for main container to complete
		err := wfExecutor.Wait(ctx)
		if err != nil {
			wfExecutor.AddError(err)
		}
	}()
	// Capture output script result
	err := wfExecutor.CaptureScriptResult(ctx)
	if err != nil {
		wfExecutor.AddError(err)
	}

	// Saving output parameters
	err = wfExecutor.SaveParameters(ctx)
	if err != nil {
		wfExecutor.AddError(err)
	}
	// Saving output artifacts
	err = wfExecutor.SaveArtifacts(ctx)
	if err != nil {
		fmt.Printf("got error from SaveArtifacts: %v\n", err)
		wfExecutor.AddError(err)
	}
	fmt.Println("deletethis: about to SaveLogs")

	wfExecutor.SaveLogs(ctx)
	return wfExecutor.HasError()
}
