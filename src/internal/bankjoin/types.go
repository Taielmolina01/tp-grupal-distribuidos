package bankjoin

type Config struct {
	Id                int
	MomHost           string
	MomPort           int
	LeftInputQueue    string
	RightInputQueue   string
	OutputQueue       string
	LeftEofsExpected  int
	RightEofsExpected int
}
