package worker

type Worker interface {
	// Starts the worker itself, with its input and output queues.
	Run()
	// Handles the OS signals of the worker.
	HandleSignals()
}
