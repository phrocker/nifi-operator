package controller

import (
	"github.com/sburges/nifi-operator/pkg/controller/nifi"
)

func init() {
	// AddToManagerFuncs is a list of functions to create controllers and add them to a manager.
	AddToManagerFuncs = append(AddToManagerFuncs, nifi.Add)
}
