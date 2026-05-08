module verify_output

go 1.24.1

require (
	github.com/7574-sistemas-distribuidos/tp-coordinacion v0.0.0
	gopkg.in/yaml.v3 v3.0.1
)

require github.com/google/uuid v1.6.0 // indirect

replace github.com/7574-sistemas-distribuidos/tp-coordinacion => ./src
