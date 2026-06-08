package join

type JoinConfig struct {
	Id                int
	MomHost           string
	MomPort           int
	LeftInputQueue    string
	RightInputQueue   string
	OutputQueue       string
	LeftEofsExpected  int
	RightEofsExpected int
}
