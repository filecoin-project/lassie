package types

/*
	Ripped from lotus chain types to avoid the lotus dependency.
*/

import (
	"math/big"
	"strings"

	gostatetypesbig "github.com/filecoin-project/go-state-types/big"
)

const FilecoinPrecision = uint64(1_000_000_000_000_000_000)

type FIL gostatetypesbig.Int

func (f FIL) String() string {
	return f.Unitless() + " FIL"
}

func (f FIL) Unitless() string {
	r := new(big.Rat).SetFrac(f.Int, big.NewInt(int64(FilecoinPrecision)))
	if r.Sign() == 0 {
		return "0"
	}
	return strings.TrimRight(strings.TrimRight(r.FloatString(18), "0"), ".")
}
