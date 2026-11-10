package wasmkeep

import (
	"strconv"

	"go.bytecodealliance.org/cm"

	"github.com/wasmkeep/wasmkeep/sdks/go/internal/gen/wasmkeep/actor/storage"
	"github.com/wasmkeep/wasmkeep/sdks/go/internal/gen/wasmkeep/actor/types"
)

// SQLError is a statement that failed.
type SQLError struct {
	// Kind is the host's sql-error-kind: "syntax", "constraint", "bind",
	// "multiple-statements", "quota-exceeded", "not-allowed" or
	// "internal".
	Kind string
	// Message is the detail, usually SQLite's own message.
	Message string
}

func (e *SQLError) Error() string { return e.Message }

// Rows is a query's result: column names, then one slice of values per
// row, in column order. A value is nil (NULL), int64, float64, string or
// []byte, as SQLite stored it.
type Rows struct {
	Columns []string
	Values  [][]any
}

// Len is the number of rows.
func (r *Rows) Len() int { return len(r.Values) }

// Column is the index of the column name, or -1.
func (r *Rows) Column(name string) int {
	for i, c := range r.Columns {
		if c == name {
			return i
		}
	}
	return -1
}

// Get is the value at row, col; nil past the end.
func (r *Rows) Get(row, col int) any {
	if row < 0 || row >= len(r.Values) || col < 0 || col >= len(r.Values[row]) {
		return nil
	}
	return r.Values[row][col]
}

// Int is the value at row, col as an integer: a REAL is truncated,
// anything else 0 (cast in SQL to read TEXT as a number).
func (r *Rows) Int(row, col int) int64 {
	switch v := r.Get(row, col).(type) {
	case int64:
		return v
	case float64:
		return int64(v)
	}
	return 0
}

// Float is the value at row, col as a float: an INTEGER converted,
// anything else 0.
func (r *Rows) Float(row, col int) float64 {
	switch v := r.Get(row, col).(type) {
	case float64:
		return v
	case int64:
		return float64(v)
	}
	return 0
}

// String is the value at row, col as text: a BLOB's bytes, a number
// formatted, NULL "".
func (r *Rows) String(row, col int) string {
	switch v := r.Get(row, col).(type) {
	case string:
		return v
	case []byte:
		return string(v)
	case int64:
		return strconv.FormatInt(v, 10)
	case float64:
		return strconv.FormatFloat(v, 'g', -1, 64)
	}
	return ""
}

// Bytes is the value at row, col as bytes: TEXT's bytes, anything else
// nil.
func (r *Rows) Bytes(row, col int) []byte {
	switch v := r.Get(row, col).(type) {
	case []byte:
		return v
	case string:
		return []byte(v)
	}
	return nil
}

// IsNull reports whether the value at row, col is NULL (or past the end).
func (r *Rows) IsNull(row, col int) bool { return r.Get(row, col) == nil }

// Exec runs one statement that returns no rows and returns the number of
// rows it changed. It returns once the write is committed to the local
// database. Parameters (`?`) are nil, a bool (0 or 1), an integer, a
// float, a string or a []byte.
func Exec(sql string, params ...any) (uint64, error) {
	p, err := toValues(params)
	if err != nil {
		return 0, err
	}
	r := storage.Exec(sql, p)
	if e := r.Err(); e != nil {
		return 0, sqlError(e)
	}
	return *r.OK(), nil
}

// ExecDurable is [Exec], returning only once the write is durable:
// replicated to the bucket when the node has one, fsynced locally when it
// does not. Inside a transaction it takes only COMMIT (or END), and makes
// the whole transaction durable.
func ExecDurable(sql string, params ...any) (uint64, error) {
	p, err := toValues(params)
	if err != nil {
		return 0, err
	}
	r := storage.ExecDurable(sql, p)
	if e := r.Err(); e != nil {
		return 0, sqlError(e)
	}
	return *r.OK(), nil
}

// Query runs one statement and returns its rows.
func Query(sql string, params ...any) (*Rows, error) {
	p, err := toValues(params)
	if err != nil {
		return nil, err
	}
	r := storage.Query(sql, p)
	if e := r.Err(); e != nil {
		return nil, sqlError(e)
	}
	return fromRows(r.OK()), nil
}

// QueryInt runs a query and returns the first column of its first row as
// an integer; a query with no rows is an error.
func QueryInt(sql string, params ...any) (int64, error) {
	rows, err := Query(sql, params...)
	if err != nil {
		return 0, err
	}
	if rows.Len() == 0 {
		return 0, &SQLError{Kind: "internal", Message: "query returned no rows"}
	}
	return rows.Int(0, 0), nil
}

// DeleteSelf deletes this actor once the current event ends: its database
// is removed and its pending alarm cleared. The next event to its address
// starts a fresh actor.
func DeleteSelf() { storage.DeleteSelf() }

func sqlError(e *types.SQLError) error {
	return &SQLError{Kind: e.Kind.String(), Message: e.Message}
}

func toValues(params []any) (cm.List[types.Value], error) {
	if len(params) == 0 {
		return cm.List[types.Value]{}, nil
	}
	out := make([]types.Value, len(params))
	for i, p := range params {
		v, ok := toValue(p)
		if !ok {
			return cm.List[types.Value]{}, &SQLError{
				Kind:    "bind",
				Message: "parameter " + strconv.Itoa(i+1) + ": unsupported type",
			}
		}
		out[i] = v
	}
	return cm.ToList(out), nil
}

func toValue(p any) (types.Value, bool) {
	switch v := p.(type) {
	case nil:
		return types.ValueNull(), true
	case bool:
		if v {
			return types.ValueInteger(1), true
		}
		return types.ValueInteger(0), true
	case int:
		return types.ValueInteger(int64(v)), true
	case int8:
		return types.ValueInteger(int64(v)), true
	case int16:
		return types.ValueInteger(int64(v)), true
	case int32:
		return types.ValueInteger(int64(v)), true
	case int64:
		return types.ValueInteger(v), true
	case uint:
		return uintValue(uint64(v))
	case uint8:
		return types.ValueInteger(int64(v)), true
	case uint16:
		return types.ValueInteger(int64(v)), true
	case uint32:
		return types.ValueInteger(int64(v)), true
	case uint64:
		return uintValue(v)
	case float32:
		return types.ValueReal(float64(v)), true
	case float64:
		return types.ValueReal(v), true
	case string:
		return types.ValueText(v), true
	case []byte:
		return types.ValueBlob(cm.ToList(v)), true
	}
	return types.Value{}, false
}

// uintValue refuses what an SQLite integer (signed 64-bit) cannot hold.
func uintValue(v uint64) (types.Value, bool) {
	if v > 1<<63-1 {
		return types.Value{}, false
	}
	return types.ValueInteger(int64(v)), true
}

func fromRows(r *types.Rows) *Rows {
	out := &Rows{Columns: append([]string(nil), r.Columns.Slice()...)}
	for _, row := range r.Values.Slice() {
		values := make([]any, row.Len())
		for i, v := range row.Slice() {
			values[i] = fromValue(v)
		}
		out.Values = append(out.Values, values)
	}
	return out
}

func fromValue(v types.Value) any {
	if n := v.Integer(); n != nil {
		return *n
	}
	if f := v.Real(); f != nil {
		return *f
	}
	if s := v.Text(); s != nil {
		return *s
	}
	if b := v.Blob(); b != nil {
		return append([]byte(nil), b.Slice()...)
	}
	return nil
}
