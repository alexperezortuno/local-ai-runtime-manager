package capacity

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
)

type ggufMetadata struct {
	Layers      uint64
	Embedding   uint64
	KVHeads     uint64
	KeyLength   uint64
	ValueLength uint64
}

type ggufReader struct{ r *os.File }

func readGGUFMetadata(path string) (ggufMetadata, error) {
	file, err := os.Open(path)
	if err != nil {
		return ggufMetadata{}, err
	}
	defer file.Close()
	reader := &ggufReader{r: file}
	magic := make([]byte, 4)
	if _, err := io.ReadFull(file, magic); err != nil || string(magic) != "GGUF" {
		return ggufMetadata{}, errors.New("magic GGUF inválido")
	}
	version, err := reader.u32()
	if err != nil {
		return ggufMetadata{}, err
	}
	if version < 1 || version > 3 {
		return ggufMetadata{}, fmt.Errorf("versión GGUF no soportada: %d", version)
	}
	_, err = reader.u64()
	if err != nil {
		return ggufMetadata{}, err
	}
	count, err := reader.u64()
	if err != nil {
		return ggufMetadata{}, err
	}
	if count > 1<<20 {
		return ggufMetadata{}, errors.New("cantidad de metadata GGUF fuera de límites")
	}
	values := make(map[string]any, count)
	for i := uint64(0); i < count; i++ {
		key, err := reader.str()
		if err != nil {
			return ggufMetadata{}, err
		}
		value, err := reader.value()
		if err != nil {
			return ggufMetadata{}, err
		}
		values[key] = value
	}
	metadata := ggufMetadata{
		Layers:      valueUint(values, "block_count"),
		Embedding:   valueUint(values, "embedding_length"),
		KVHeads:     valueUint(values, "attention.head_count_kv"),
		KeyLength:   valueUint(values, "attention.key_length"),
		ValueLength: valueUint(values, "attention.value_length"),
	}
	if metadata.KVHeads == 0 {
		metadata.KVHeads = valueUint(values, "attention.head_count")
	}
	if metadata.KeyLength == 0 {
		head := valueUint(values, "attention.head_count")
		if head > 0 {
			metadata.KeyLength = metadata.Embedding / head
		}
	}
	if metadata.ValueLength == 0 {
		metadata.ValueLength = metadata.KeyLength
	}
	return metadata, nil
}

func valueUint(values map[string]any, suffix string) uint64 {
	for key, value := range values {
		if key != suffix && !strings.HasSuffix(key, "."+suffix) {
			continue
		}
		switch n := value.(type) {
		case uint8:
			return uint64(n)
		case uint64:
			return n
		case int64:
			if n > 0 {
				return uint64(n)
			}
		case float64:
			if n > 0 {
				return uint64(n)
			}
		}
	}
	return 0
}

func (g *ggufReader) u8() (uint8, error) {
	var value uint8
	err := binary.Read(g.r, binary.LittleEndian, &value)
	return value, err
}
func (g *ggufReader) u16() (uint16, error) {
	var value uint16
	err := binary.Read(g.r, binary.LittleEndian, &value)
	return value, err
}
func (g *ggufReader) u32() (uint32, error) {
	var value uint32
	err := binary.Read(g.r, binary.LittleEndian, &value)
	return value, err
}
func (g *ggufReader) u64() (uint64, error) {
	var value uint64
	err := binary.Read(g.r, binary.LittleEndian, &value)
	return value, err
}
func (g *ggufReader) str() (string, error) {
	n, err := g.u64()
	if err != nil {
		return "", err
	}
	if n > 1<<24 {
		return "", errors.New("string GGUF fuera de límites")
	}
	data := make([]byte, n)
	_, err = io.ReadFull(g.r, data)
	return string(data), err
}

func (g *ggufReader) value() (any, error) {
	typ, err := g.u32()
	if err != nil {
		return nil, err
	}
	switch typ {
	case 0:
		return g.u8()
	case 1:
		var v int8
		err = binary.Read(g.r, binary.LittleEndian, &v)
		return int64(v), err
	case 2:
		v, e := g.u16()
		return uint64(v), e
	case 3:
		var v int16
		err = binary.Read(g.r, binary.LittleEndian, &v)
		return int64(v), err
	case 4:
		v, e := g.u32()
		return uint64(v), e
	case 5:
		var v int32
		err = binary.Read(g.r, binary.LittleEndian, &v)
		return int64(v), err
	case 6:
		var v float32
		err = binary.Read(g.r, binary.LittleEndian, &v)
		return float64(v), err
	case 7:
		return g.u8()
	case 8:
		return g.str()
	case 9:
		elementType, err := g.u32()
		if err != nil {
			return nil, err
		}
		count, err := g.u64()
		if err != nil {
			return nil, err
		}
		if count > 1<<20 {
			return nil, errors.New("array GGUF fuera de límites")
		}
		for i := uint64(0); i < count; i++ {
			if err := g.skipValue(elementType); err != nil {
				return nil, err
			}
		}
		return nil, nil
	case 10:
		return g.u64()
	case 11:
		var v int64
		err = binary.Read(g.r, binary.LittleEndian, &v)
		return v, err
	case 12:
		var v float64
		err = binary.Read(g.r, binary.LittleEndian, &v)
		return v, err
	default:
		return nil, fmt.Errorf("tipo de metadata GGUF desconocido: %d", typ)
	}
}

func (g *ggufReader) skipValue(typ uint32) error {
	switch typ {
	case 0, 1, 7:
		_, err := g.u8()
		return err
	case 2, 3:
		_, err := g.u16()
		return err
	case 4, 5, 6:
		_, err := g.u32()
		return err
	case 8:
		_, err := g.str()
		return err
	case 10, 11, 12:
		_, err := g.u64()
		return err
	default:
		return fmt.Errorf("array GGUF no escalar soportado: %d", typ)
	}
}
