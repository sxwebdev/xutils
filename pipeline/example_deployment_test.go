package pipeline_test

import (
	"context"
	"fmt"
	"time"

	"github.com/sxwebdev/xutils/pipeline"
)

// This example demonstrates a CI/CD deployment pipeline that:
// 1. Validates the configuration
// 2. Runs tests
// 3. Builds artifacts
// 4. Branches based on environment (staging vs production)
// 5. Provisions infrastructure (with rollback on failure)
// 6. Deploys the application
// 7. Waits for health checks
// 8. Sends a notification
//
// The pipeline survives process restarts and automatically rolls back
// provisioned infrastructure if deployment fails.

func Example_deploymentPipeline() {
	// --- Step implementations ---

	validateConfig := func(ctx context.Context, data pipeline.DataAccessor) error {
		fmt.Println("validating deployment config...")
		data.Set("app_version", "v2.4.1")
		data.Set("image", "registry.example.com/myapp:v2.4.1")
		return nil
	}

	runTests := func(ctx context.Context, data pipeline.DataAccessor) error {
		fmt.Println("running test suite...")
		data.Set("tests_passed", true)
		return nil
	}

	buildArtifact := func(ctx context.Context, data pipeline.DataAccessor) error {
		image, _ := pipeline.GetData[string](data, "image")
		fmt.Printf("building docker image %s...\n", image)
		data.Set("artifact_id", "build-20240315-001")
		return nil
	}

	decideEnvironment := func(ctx context.Context, data pipeline.DataAccessor) (string, error) {
		// In a real pipeline, this would come from config or API.
		return "production", nil
	}

	provisionInfra := func(ctx context.Context, data pipeline.DataAccessor) error {
		fmt.Println("provisioning production infrastructure...")
		data.Set("infra_id", "infra-prod-42")
		return nil
	}

	destroyInfra := func(ctx context.Context, data pipeline.DataAccessor) error {
		infraID, _ := pipeline.GetData[string](data, "infra_id")
		fmt.Printf("rolling back: destroying infrastructure %s...\n", infraID)
		return nil
	}

	deployApp := func(ctx context.Context, data pipeline.DataAccessor) error {
		artifactID, _ := pipeline.GetData[string](data, "artifact_id")
		infraID, _ := pipeline.GetData[string](data, "infra_id")
		fmt.Printf("deploying %s to %s...\n", artifactID, infraID)
		data.Set("deployment_id", "deploy-789")
		return nil
	}

	// Simulates checking deployment health (poll step).
	healthCheckCount := 0
	waitHealthCheck := func(ctx context.Context, data pipeline.DataAccessor) (bool, time.Duration, error) {
		healthCheckCount++
		deploymentID, _ := pipeline.GetData[string](data, "deployment_id")
		fmt.Printf("checking health of %s (attempt %d)...\n", deploymentID, healthCheckCount)

		if healthCheckCount >= 2 {
			fmt.Println("health check passed!")
			return true, 0, nil
		}

		return false, 100 * time.Millisecond, nil // retry in 100ms
	}

	directDeploy := func(ctx context.Context, data pipeline.DataAccessor) error {
		fmt.Println("deploying directly to staging...")
		return nil
	}

	sendNotification := func(ctx context.Context, data pipeline.DataAccessor) error {
		version, _ := pipeline.GetData[string](data, "app_version")
		fmt.Printf("deployment of %s completed successfully!\n", version)
		return nil
	}

	// --- Pipeline definition ---

	p := &pipeline.Pipeline{
		Name: "deploy_myapp",
		Steps: []pipeline.Step{
			pipeline.Action("validate_config", validateConfig),
			pipeline.Action("run_tests", runTests),
			pipeline.Action("build_artifact", buildArtifact),

			pipeline.Branch("select_environment", decideEnvironment, map[string][]pipeline.Step{
				"production": {
					pipeline.Action("provision_infra", provisionInfra,
						pipeline.WithCompensate(destroyInfra)),
					pipeline.Action("deploy_app", deployApp),
					pipeline.Poll("wait_health_check", waitHealthCheck,
						pipeline.WithMaxPollDuration(5*time.Minute)),
				},
				"staging": {
					pipeline.Action("direct_deploy", directDeploy),
				},
			}),

			pipeline.Action("send_notification", sendNotification),
		},
	}

	// --- Execution ---

	executor := pipeline.NewExecutor()

	// First run — will snooze on health check.
	state, err := executor.Run(context.Background(), p, pipeline.RunState{})
	if err != nil {
		// ErrSnooze means poll is waiting.
		fmt.Printf("snoozing for: %v\n", err)
	}

	// Resume — health check passes, pipeline completes.
	state, err = executor.Run(context.Background(), p, state)
	if err != nil {
		fmt.Printf("error: %v\n", err)
		return
	}

	fmt.Printf("final status: %s\n", state.Status)

	// Output:
	// validating deployment config...
	// running test suite...
	// building docker image registry.example.com/myapp:v2.4.1...
	// provisioning production infrastructure...
	// deploying build-20240315-001 to infra-prod-42...
	// checking health of deploy-789 (attempt 1)...
	// snoozing for: pipeline: snooze for 100ms
	// checking health of deploy-789 (attempt 2)...
	// health check passed!
	// deployment of v2.4.1 completed successfully!
	// final status: completed
}
