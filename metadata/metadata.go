package metadata

import (
	"fmt"
	"mdata/utils"
	"os"
)

var CredentialsFile = "/etc/secrets/gcp.json"

func Initialize() error {
	// default value
	CredentialsFile = "/etc/secrets/gcp.json"

	environment := os.Getenv("ENVIRONMENT")
	if environment != "" {
		if environment == "LOCAL" {
			CredentialsFile = "gcp.json"
		}
	}

	if !utils.FileExists(CredentialsFile) {
		return fmt.Errorf("CredentialsFile : ( %v ) is missing", CredentialsFile)
	}
	return nil
}
