package binding

import (
	"encoding/xml"
	"net/http"
)

type xmlBinding struct {
}

func (xmlBinding) Name() string {
	return "xml"
}

func (b xmlBinding) Bind(r *http.Request, obj any) error {
	if r.Body == nil {
		return nil
	}
	decoder := xml.NewDecoder(r.Body)
	if err := decoder.Decode(obj); err != nil {
		return err
	}
	return validate(obj)
}
