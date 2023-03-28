package binding

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"reflect"
)

type jsonBinding struct {
	DisallowUnknownFields bool
	IsValidate            bool
}

func (jsonBinding) Name() string {
	return "json"
}

func (b jsonBinding) Bind(r *http.Request, obj any) error {
	body := r.Body
	//post传参的内容 是放在 body中的
	if body == nil {
		return errors.New("invalid request")
	}
	decoder := json.NewDecoder(body)
	if b.DisallowUnknownFields {
		decoder.DisallowUnknownFields()
	}
	if b.IsValidate {
		err := validateParam(obj, decoder)
		if err != nil {
			return err
		}
	} else {
		err := decoder.Decode(obj)
		if err != nil {
			return err
		}
	}
	return validate(obj)
}

func validateParam(obj any, decoder *json.Decoder) error {

	//反射
	valueOf := reflect.ValueOf(obj)
	//判断其是否为指针类型
	if valueOf.Kind() != reflect.Pointer {
		return errors.New("This argument must have a pointer type")
	}
	elem := valueOf.Elem().Interface()
	of := reflect.ValueOf(elem)

	switch of.Kind() {
	case reflect.Struct:
		return checkParam(of, obj, decoder)
	case reflect.Slice, reflect.Array:
		elem := of.Type().Elem()
		if elem.Kind() == reflect.Struct {
			return checkParamSlice(elem, obj, decoder)
		}
	default:
		_ = decoder.Decode(obj)
	}
	return nil
}

func checkParamSlice(of reflect.Type, obj any, decoder *json.Decoder) error {
	mapValue := make([]map[string]interface{}, 0)
	_ = decoder.Decode(&mapValue)
	for i := 0; i < of.NumField(); i++ {
		field := of.Field(i)
		name := field.Name
		jsonName := field.Tag.Get("json")
		if jsonName != "" {
			name = jsonName
		}
		required := field.Tag.Get("msgo")
		for _, v := range mapValue {
			value := v[name]
			if value == nil && required == "required" {
				return errors.New(fmt.Sprintf("filed [%s] is not exist,because [%s] is required", jsonName, jsonName))
			}
		}
	}
	b, _ := json.Marshal(mapValue)
	_ = json.Unmarshal(b, obj)
	return nil
}

func checkParam(of reflect.Value, obj any, decoder *json.Decoder) error {
	//解析为map，然后根据map中的key 进行比对
	//判断类型 结构体 才能解析为map
	mapValue := make(map[string]interface{})
	_ = decoder.Decode(&mapValue)
	for i := 0; i < of.NumField(); i++ {
		field := of.Type().Field(i)
		name := field.Name
		jsonName := field.Tag.Get("json")
		if jsonName != "" {
			name = jsonName
		}
		required := field.Tag.Get("msgo")
		value := mapValue[name]
		if value == nil && required == "required" {
			return errors.New(fmt.Sprintf("filed [%s] is not exist,because [%s] is required", jsonName, jsonName))
		}
	}
	b, _ := json.Marshal(mapValue)
	_ = json.Unmarshal(b, obj)
	return nil
}
