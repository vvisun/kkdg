package kkactor

// PID represents a process ID (actor identifier).
type PID struct {
	id     string
	system *ActorSystem
}

// String returns the string representation of the PID.
func (pid *PID) String() string {
	return pid.id
}

// Tell sends a message to this actor (fire-and-forget).
func (pid *PID) Tell(message interface{}) {
	if pid == nil || pid.system == nil {
		return
	}
	pid.system.send(pid, message, nil)
}
