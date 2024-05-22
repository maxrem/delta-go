package parquet

import (
	"fmt"
	"github.com/fraugster/parquet-go/floor/interfaces"
	"reflect"
)

func UnmarshalString(obj interfaces.UnmarshalObject, fieldName string, setter func(s string)) error {
	if _, ok := obj.GetData()[fieldName]; ok {
		b, err := obj.GetField(fieldName).ByteArray()
		if err != nil {
			return err
		}
		setter(string(b))
	}
	return nil
}

func UnmarshalInt64(obj interfaces.UnmarshalObject, fieldName string, setter func(s int64)) error {
	if _, ok := obj.GetData()[fieldName]; ok {
		b, err := obj.GetField(fieldName).Int64()
		if err != nil {
			return err
		}
		setter(b)
	}
	return nil
}

func UnmarshalInt32(obj interfaces.UnmarshalObject, fieldName string, setter func(s int32)) error {
	if _, ok := obj.GetData()[fieldName]; ok {
		b, err := obj.GetField(fieldName).Int32()
		if err != nil {
			return err
		}
		setter(b)
	}
	return nil
}

func UnmarshalBool(obj interfaces.UnmarshalObject, fieldName string, setter func(s bool)) error {
	if _, ok := obj.GetData()[fieldName]; ok {
		b, err := obj.GetField(fieldName).Bool()
		if err != nil {
			return err
		}
		setter(b)
	}
	return nil
}

func UnmarshalList(obj interfaces.UnmarshalObject, fieldName string, setter func([]string)) error {
	v, ok := obj.GetData()[fieldName]
	if !ok {
		return nil
	}
	// avoid empty list
	arr := v.(map[string]interface{})
	if len(arr) == 0 {
		return nil
	}

	l, err := obj.GetField(fieldName).List()
	if err != nil {
		return err
	}

	var res []string
	for l.Next() {
		v, err := l.Value()
		if err != nil {
			return err
		}
		b, err := v.ByteArray()
		if err != nil {
			return err
		}
		res = append(res, string(b))
	}
	setter(res)

	return nil
}

func UnmarshalMapString(obj interfaces.UnmarshalObject, fieldName string, setter func(map[string]string)) error {
	v, ok := obj.GetData()[fieldName]
	if !ok {
		return nil
	}
	// avoid empty map
	vm := v.(map[string]interface{})
	if len(vm) == 0 {
		return nil
	}

	m, err := obj.GetField(fieldName).Map()
	if err != nil {
		return err
	}

	res := make(map[string]string)
	for m.Next() {
		k, err := m.Key()
		if err != nil {
			return err
		}
		key, err := k.ByteArray()
		if err != nil {
			return err
		}
		v, err := m.Value()
		if err != nil {
			return err
		}
		val, err := v.ByteArray()
		if err != nil {
			return err
		}
		res[string(key)] = string(val)
	}
	setter(res)
	return nil
}

func UnmarshalMapAny(obj interfaces.UnmarshalObject, fieldName string, setter func(map[string]any)) error {
	v, ok := obj.GetData()[fieldName]
	if !ok {
		return nil
	}
	// avoid empty map
	vm := v.(map[string]interface{})
	if len(vm) == 0 {
		return nil
	}

	m, err := obj.GetField(fieldName).Map()
	if err != nil {
		return err
	}

	res := make(map[string]any)
	for m.Next() {
		k, err := m.Key()
		if err != nil {
			return err
		}
		key, err := k.ByteArray()
		if err != nil {
			return err
		}
		v, err := m.Value()
		if err != nil {
			return err
		}
		valueType := reflect.TypeOf(v)
		switch valueType.Kind() {
		case reflect.String:
			val, err := v.ByteArray()
			if err != nil {
				return err
			}
			res[string(key)] = string(val)
		case reflect.Bool:
			val, err := v.Bool()
			if err != nil {
				return err
			}
			res[string(key)] = val
		case reflect.Int32:
			val, err := v.Int32()
			if err != nil {
				return err
			}
			res[string(key)] = val
		case reflect.Int64:
			val, err := v.Int64()
			if err != nil {
				return err
			}
			res[string(key)] = val
		case reflect.Float32:
			val, err := v.Float32()
			if err != nil {
				return err
			}
			res[string(key)] = val
		case reflect.Float64:
			val, err := v.Float64()
			if err != nil {
				return err
			}
			res[string(key)] = val
		default:
			panic(fmt.Sprintf("unsupported type %v with value %v", valueType.Kind(), v))
		}
	}
	setter(res)
	return nil
}

func MarshalMapString(obj interfaces.MarshalObject, fieldName string, m map[string]string) error {
	if m == nil {
		return nil
	}
	mo := obj.AddField(fieldName).Map()
	for k, v := range m {
		elem := mo.Add()
		elem.Key().SetByteArray([]byte(k))
		elem.Value().SetByteArray([]byte(v))
	}
	return nil
}

func MarshalMapAny(obj interfaces.MarshalObject, fieldName string, m map[string]any) error {
	if m == nil {
		return nil
	}
	mo := obj.AddField(fieldName).Map()
	for k, v := range m {
		elem := mo.Add()
		elem.Key().SetByteArray([]byte(k))
		valueType := reflect.TypeOf(v)
		switch valueType.Kind() {
		case reflect.String:
			elem.Value().SetByteArray([]byte(v.(string)))
		case reflect.Bool:
			elem.Value().SetBool(v.(bool))
		case reflect.Int32:
			elem.Value().SetInt32(v.(int32))
		case reflect.Int64:
			elem.Value().SetInt64(v.(int64))
		case reflect.Float32:
			elem.Value().SetFloat32(v.(float32))
		case reflect.Float64:
			elem.Value().SetFloat64(v.(float64))
		default:
			panic(fmt.Sprintf("unsupported type %v with value %v", valueType.Kind(), v))
		}
	}
	return nil
}

func MarshalList(obj interfaces.MarshalObject, fieldName string, arr []string) error {
	if len(arr) == 0 {
		return nil
	}
	l := obj.AddField(fieldName).List()
	for _, a := range arr {
		elem := l.Add()
		elem.SetByteArray([]byte(a))
	}
	return nil
}
