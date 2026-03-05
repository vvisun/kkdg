package ptoexam

type (
	Msg1Req struct {
		ID   int
		Data string
	}
	Msg1Resp struct {
		ID   int
		Data string
	}
	Msg2Broadcast struct {
		ID   int
		Data string
	}
)
