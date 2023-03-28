package mserror

type MsError struct {
	err    error
	ErrFuc ErrorFuc
}

func Default() *MsError {
	return &MsError{}
}
func (e *MsError) Error() string {
	return e.err.Error()
}

func (e *MsError) Put(err error) {
	e.check(err)
}

func (e *MsError) check(err error) {
	if err != nil {
		e.err = err
		panic(e)
	}
}

type ErrorFuc func(msError *MsError)

//暴露一个方法 让用户自定义

func (e *MsError) Result(errFuc ErrorFuc) {
	e.ErrFuc = errFuc
}
func (e *MsError) ExecResult() {
	e.ErrFuc(e)
}
