// Code generated by github.com/whyrusleeping/cbor-gen. DO NOT EDIT.

package types

import (
	"fmt"
	"io"
	"math"
	"sort"

	paych "github.com/filecoin-project/go-state-types/builtin/v8/paych"
	cid "github.com/ipfs/go-cid"
	cbg "github.com/whyrusleeping/cbor-gen"
	xerrors "golang.org/x/xerrors"
)

var _ = xerrors.Errorf
var _ = cid.Undef
var _ = math.E
var _ = sort.Sort

func (t *Query) MarshalCBOR(w io.Writer) error {
	if t == nil {
		_, err := w.Write(cbg.CborNull)
		return err
	}

	cw := cbg.NewCborWriter(w)

	if _, err := cw.Write([]byte{162}); err != nil {
		return err
	}

	// t.PayloadCID (cid.Cid) (struct)
	if len("PayloadCID") > cbg.MaxLength {
		return xerrors.Errorf("Value in field \"PayloadCID\" was too long")
	}

	if err := cw.WriteMajorTypeHeader(cbg.MajTextString, uint64(len("PayloadCID"))); err != nil {
		return err
	}
	if _, err := io.WriteString(w, string("PayloadCID")); err != nil {
		return err
	}

	if err := cbg.WriteCid(cw, t.PayloadCID); err != nil {
		return xerrors.Errorf("failed to write cid field t.PayloadCID: %w", err)
	}

	// t.QueryParams (types.QueryParams) (struct)
	if len("QueryParams") > cbg.MaxLength {
		return xerrors.Errorf("Value in field \"QueryParams\" was too long")
	}

	if err := cw.WriteMajorTypeHeader(cbg.MajTextString, uint64(len("QueryParams"))); err != nil {
		return err
	}
	if _, err := io.WriteString(w, string("QueryParams")); err != nil {
		return err
	}

	if err := t.QueryParams.MarshalCBOR(cw); err != nil {
		return err
	}
	return nil
}

func (t *Query) UnmarshalCBOR(r io.Reader) (err error) {
	*t = Query{}

	cr := cbg.NewCborReader(r)

	maj, extra, err := cr.ReadHeader()
	if err != nil {
		return err
	}
	defer func() {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
	}()

	if maj != cbg.MajMap {
		return fmt.Errorf("cbor input should be of type map")
	}

	if extra > cbg.MaxLength {
		return fmt.Errorf("Query: map struct too large (%d)", extra)
	}

	var name string
	n := extra

	for i := uint64(0); i < n; i++ {

		{
			sval, err := cbg.ReadString(cr)
			if err != nil {
				return err
			}

			name = string(sval)
		}

		switch name {
		// t.PayloadCID (cid.Cid) (struct)
		case "PayloadCID":

			{

				c, err := cbg.ReadCid(cr)
				if err != nil {
					return xerrors.Errorf("failed to read cid field t.PayloadCID: %w", err)
				}

				t.PayloadCID = c

			}
			// t.QueryParams (types.QueryParams) (struct)
		case "QueryParams":

			{

				if err := t.QueryParams.UnmarshalCBOR(cr); err != nil {
					return xerrors.Errorf("unmarshaling t.QueryParams: %w", err)
				}

			}

		default:
			// Field doesn't exist on this type, so ignore it
			cbg.ScanForLinks(r, func(cid.Cid) {})
		}
	}

	return nil
}
func (t *QueryResponse) MarshalCBOR(w io.Writer) error {
	if t == nil {
		_, err := w.Write(cbg.CborNull)
		return err
	}

	cw := cbg.NewCborWriter(w)

	if _, err := cw.Write([]byte{169}); err != nil {
		return err
	}

	// t.Size (uint64) (uint64)
	if len("Size") > cbg.MaxLength {
		return xerrors.Errorf("Value in field \"Size\" was too long")
	}

	if err := cw.WriteMajorTypeHeader(cbg.MajTextString, uint64(len("Size"))); err != nil {
		return err
	}
	if _, err := io.WriteString(w, string("Size")); err != nil {
		return err
	}

	if err := cw.WriteMajorTypeHeader(cbg.MajUnsignedInt, uint64(t.Size)); err != nil {
		return err
	}

	// t.Status (types.QueryResponseStatus) (uint64)
	if len("Status") > cbg.MaxLength {
		return xerrors.Errorf("Value in field \"Status\" was too long")
	}

	if err := cw.WriteMajorTypeHeader(cbg.MajTextString, uint64(len("Status"))); err != nil {
		return err
	}
	if _, err := io.WriteString(w, string("Status")); err != nil {
		return err
	}

	if err := cw.WriteMajorTypeHeader(cbg.MajUnsignedInt, uint64(t.Status)); err != nil {
		return err
	}

	// t.Message (string) (string)
	if len("Message") > cbg.MaxLength {
		return xerrors.Errorf("Value in field \"Message\" was too long")
	}

	if err := cw.WriteMajorTypeHeader(cbg.MajTextString, uint64(len("Message"))); err != nil {
		return err
	}
	if _, err := io.WriteString(w, string("Message")); err != nil {
		return err
	}

	if len(t.Message) > cbg.MaxLength {
		return xerrors.Errorf("Value in field t.Message was too long")
	}

	if err := cw.WriteMajorTypeHeader(cbg.MajTextString, uint64(len(t.Message))); err != nil {
		return err
	}
	if _, err := io.WriteString(w, string(t.Message)); err != nil {
		return err
	}

	// t.UnsealPrice (big.Int) (struct)
	if len("UnsealPrice") > cbg.MaxLength {
		return xerrors.Errorf("Value in field \"UnsealPrice\" was too long")
	}

	if err := cw.WriteMajorTypeHeader(cbg.MajTextString, uint64(len("UnsealPrice"))); err != nil {
		return err
	}
	if _, err := io.WriteString(w, string("UnsealPrice")); err != nil {
		return err
	}

	if err := t.UnsealPrice.MarshalCBOR(cw); err != nil {
		return err
	}

	// t.PieceCIDFound (types.QueryItemStatus) (uint64)
	if len("PieceCIDFound") > cbg.MaxLength {
		return xerrors.Errorf("Value in field \"PieceCIDFound\" was too long")
	}

	if err := cw.WriteMajorTypeHeader(cbg.MajTextString, uint64(len("PieceCIDFound"))); err != nil {
		return err
	}
	if _, err := io.WriteString(w, string("PieceCIDFound")); err != nil {
		return err
	}

	if err := cw.WriteMajorTypeHeader(cbg.MajUnsignedInt, uint64(t.PieceCIDFound)); err != nil {
		return err
	}

	// t.PaymentAddress (address.Address) (struct)
	if len("PaymentAddress") > cbg.MaxLength {
		return xerrors.Errorf("Value in field \"PaymentAddress\" was too long")
	}

	if err := cw.WriteMajorTypeHeader(cbg.MajTextString, uint64(len("PaymentAddress"))); err != nil {
		return err
	}
	if _, err := io.WriteString(w, string("PaymentAddress")); err != nil {
		return err
	}

	if err := t.PaymentAddress.MarshalCBOR(cw); err != nil {
		return err
	}

	// t.MinPricePerByte (big.Int) (struct)
	if len("MinPricePerByte") > cbg.MaxLength {
		return xerrors.Errorf("Value in field \"MinPricePerByte\" was too long")
	}

	if err := cw.WriteMajorTypeHeader(cbg.MajTextString, uint64(len("MinPricePerByte"))); err != nil {
		return err
	}
	if _, err := io.WriteString(w, string("MinPricePerByte")); err != nil {
		return err
	}

	if err := t.MinPricePerByte.MarshalCBOR(cw); err != nil {
		return err
	}

	// t.MaxPaymentInterval (uint64) (uint64)
	if len("MaxPaymentInterval") > cbg.MaxLength {
		return xerrors.Errorf("Value in field \"MaxPaymentInterval\" was too long")
	}

	if err := cw.WriteMajorTypeHeader(cbg.MajTextString, uint64(len("MaxPaymentInterval"))); err != nil {
		return err
	}
	if _, err := io.WriteString(w, string("MaxPaymentInterval")); err != nil {
		return err
	}

	if err := cw.WriteMajorTypeHeader(cbg.MajUnsignedInt, uint64(t.MaxPaymentInterval)); err != nil {
		return err
	}

	// t.MaxPaymentIntervalIncrease (uint64) (uint64)
	if len("MaxPaymentIntervalIncrease") > cbg.MaxLength {
		return xerrors.Errorf("Value in field \"MaxPaymentIntervalIncrease\" was too long")
	}

	if err := cw.WriteMajorTypeHeader(cbg.MajTextString, uint64(len("MaxPaymentIntervalIncrease"))); err != nil {
		return err
	}
	if _, err := io.WriteString(w, string("MaxPaymentIntervalIncrease")); err != nil {
		return err
	}

	if err := cw.WriteMajorTypeHeader(cbg.MajUnsignedInt, uint64(t.MaxPaymentIntervalIncrease)); err != nil {
		return err
	}

	return nil
}

func (t *QueryResponse) UnmarshalCBOR(r io.Reader) (err error) {
	*t = QueryResponse{}

	cr := cbg.NewCborReader(r)

	maj, extra, err := cr.ReadHeader()
	if err != nil {
		return err
	}
	defer func() {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
	}()

	if maj != cbg.MajMap {
		return fmt.Errorf("cbor input should be of type map")
	}

	if extra > cbg.MaxLength {
		return fmt.Errorf("QueryResponse: map struct too large (%d)", extra)
	}

	var name string
	n := extra

	for i := uint64(0); i < n; i++ {

		{
			sval, err := cbg.ReadString(cr)
			if err != nil {
				return err
			}

			name = string(sval)
		}

		switch name {
		// t.Size (uint64) (uint64)
		case "Size":

			{

				maj, extra, err = cr.ReadHeader()
				if err != nil {
					return err
				}
				if maj != cbg.MajUnsignedInt {
					return fmt.Errorf("wrong type for uint64 field")
				}
				t.Size = uint64(extra)

			}
			// t.Status (types.QueryResponseStatus) (uint64)
		case "Status":

			{

				maj, extra, err = cr.ReadHeader()
				if err != nil {
					return err
				}
				if maj != cbg.MajUnsignedInt {
					return fmt.Errorf("wrong type for uint64 field")
				}
				t.Status = QueryResponseStatus(extra)

			}
			// t.Message (string) (string)
		case "Message":

			{
				sval, err := cbg.ReadString(cr)
				if err != nil {
					return err
				}

				t.Message = string(sval)
			}
			// t.UnsealPrice (big.Int) (struct)
		case "UnsealPrice":

			{

				if err := t.UnsealPrice.UnmarshalCBOR(cr); err != nil {
					return xerrors.Errorf("unmarshaling t.UnsealPrice: %w", err)
				}

			}
			// t.PieceCIDFound (types.QueryItemStatus) (uint64)
		case "PieceCIDFound":

			{

				maj, extra, err = cr.ReadHeader()
				if err != nil {
					return err
				}
				if maj != cbg.MajUnsignedInt {
					return fmt.Errorf("wrong type for uint64 field")
				}
				t.PieceCIDFound = QueryItemStatus(extra)

			}
			// t.PaymentAddress (address.Address) (struct)
		case "PaymentAddress":

			{

				if err := t.PaymentAddress.UnmarshalCBOR(cr); err != nil {
					return xerrors.Errorf("unmarshaling t.PaymentAddress: %w", err)
				}

			}
			// t.MinPricePerByte (big.Int) (struct)
		case "MinPricePerByte":

			{

				if err := t.MinPricePerByte.UnmarshalCBOR(cr); err != nil {
					return xerrors.Errorf("unmarshaling t.MinPricePerByte: %w", err)
				}

			}
			// t.MaxPaymentInterval (uint64) (uint64)
		case "MaxPaymentInterval":

			{

				maj, extra, err = cr.ReadHeader()
				if err != nil {
					return err
				}
				if maj != cbg.MajUnsignedInt {
					return fmt.Errorf("wrong type for uint64 field")
				}
				t.MaxPaymentInterval = uint64(extra)

			}
			// t.MaxPaymentIntervalIncrease (uint64) (uint64)
		case "MaxPaymentIntervalIncrease":

			{

				maj, extra, err = cr.ReadHeader()
				if err != nil {
					return err
				}
				if maj != cbg.MajUnsignedInt {
					return fmt.Errorf("wrong type for uint64 field")
				}
				t.MaxPaymentIntervalIncrease = uint64(extra)

			}

		default:
			// Field doesn't exist on this type, so ignore it
			cbg.ScanForLinks(r, func(cid.Cid) {})
		}
	}

	return nil
}
func (t *DealProposal) MarshalCBOR(w io.Writer) error {
	if t == nil {
		_, err := w.Write(cbg.CborNull)
		return err
	}

	cw := cbg.NewCborWriter(w)

	if _, err := cw.Write([]byte{163}); err != nil {
		return err
	}

	// t.ID (types.DealID) (uint64)
	if len("ID") > cbg.MaxLength {
		return xerrors.Errorf("Value in field \"ID\" was too long")
	}

	if err := cw.WriteMajorTypeHeader(cbg.MajTextString, uint64(len("ID"))); err != nil {
		return err
	}
	if _, err := io.WriteString(w, string("ID")); err != nil {
		return err
	}

	if err := cw.WriteMajorTypeHeader(cbg.MajUnsignedInt, uint64(t.ID)); err != nil {
		return err
	}

	// t.Params (types.Params) (struct)
	if len("Params") > cbg.MaxLength {
		return xerrors.Errorf("Value in field \"Params\" was too long")
	}

	if err := cw.WriteMajorTypeHeader(cbg.MajTextString, uint64(len("Params"))); err != nil {
		return err
	}
	if _, err := io.WriteString(w, string("Params")); err != nil {
		return err
	}

	if err := t.Params.MarshalCBOR(cw); err != nil {
		return err
	}

	// t.PayloadCID (cid.Cid) (struct)
	if len("PayloadCID") > cbg.MaxLength {
		return xerrors.Errorf("Value in field \"PayloadCID\" was too long")
	}

	if err := cw.WriteMajorTypeHeader(cbg.MajTextString, uint64(len("PayloadCID"))); err != nil {
		return err
	}
	if _, err := io.WriteString(w, string("PayloadCID")); err != nil {
		return err
	}

	if err := cbg.WriteCid(cw, t.PayloadCID); err != nil {
		return xerrors.Errorf("failed to write cid field t.PayloadCID: %w", err)
	}

	return nil
}

func (t *DealProposal) UnmarshalCBOR(r io.Reader) (err error) {
	*t = DealProposal{}

	cr := cbg.NewCborReader(r)

	maj, extra, err := cr.ReadHeader()
	if err != nil {
		return err
	}
	defer func() {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
	}()

	if maj != cbg.MajMap {
		return fmt.Errorf("cbor input should be of type map")
	}

	if extra > cbg.MaxLength {
		return fmt.Errorf("DealProposal: map struct too large (%d)", extra)
	}

	var name string
	n := extra

	for i := uint64(0); i < n; i++ {

		{
			sval, err := cbg.ReadString(cr)
			if err != nil {
				return err
			}

			name = string(sval)
		}

		switch name {
		// t.ID (types.DealID) (uint64)
		case "ID":

			{

				maj, extra, err = cr.ReadHeader()
				if err != nil {
					return err
				}
				if maj != cbg.MajUnsignedInt {
					return fmt.Errorf("wrong type for uint64 field")
				}
				t.ID = DealID(extra)

			}
			// t.Params (types.Params) (struct)
		case "Params":

			{

				if err := t.Params.UnmarshalCBOR(cr); err != nil {
					return xerrors.Errorf("unmarshaling t.Params: %w", err)
				}

			}
			// t.PayloadCID (cid.Cid) (struct)
		case "PayloadCID":

			{

				c, err := cbg.ReadCid(cr)
				if err != nil {
					return xerrors.Errorf("failed to read cid field t.PayloadCID: %w", err)
				}

				t.PayloadCID = c

			}

		default:
			// Field doesn't exist on this type, so ignore it
			cbg.ScanForLinks(r, func(cid.Cid) {})
		}
	}

	return nil
}
func (t *DealResponse) MarshalCBOR(w io.Writer) error {
	if t == nil {
		_, err := w.Write(cbg.CborNull)
		return err
	}

	cw := cbg.NewCborWriter(w)

	if _, err := cw.Write([]byte{164}); err != nil {
		return err
	}

	// t.ID (types.DealID) (uint64)
	if len("ID") > cbg.MaxLength {
		return xerrors.Errorf("Value in field \"ID\" was too long")
	}

	if err := cw.WriteMajorTypeHeader(cbg.MajTextString, uint64(len("ID"))); err != nil {
		return err
	}
	if _, err := io.WriteString(w, string("ID")); err != nil {
		return err
	}

	if err := cw.WriteMajorTypeHeader(cbg.MajUnsignedInt, uint64(t.ID)); err != nil {
		return err
	}

	// t.Status (types.DealStatus) (uint64)
	if len("Status") > cbg.MaxLength {
		return xerrors.Errorf("Value in field \"Status\" was too long")
	}

	if err := cw.WriteMajorTypeHeader(cbg.MajTextString, uint64(len("Status"))); err != nil {
		return err
	}
	if _, err := io.WriteString(w, string("Status")); err != nil {
		return err
	}

	if err := cw.WriteMajorTypeHeader(cbg.MajUnsignedInt, uint64(t.Status)); err != nil {
		return err
	}

	// t.Message (string) (string)
	if len("Message") > cbg.MaxLength {
		return xerrors.Errorf("Value in field \"Message\" was too long")
	}

	if err := cw.WriteMajorTypeHeader(cbg.MajTextString, uint64(len("Message"))); err != nil {
		return err
	}
	if _, err := io.WriteString(w, string("Message")); err != nil {
		return err
	}

	if len(t.Message) > cbg.MaxLength {
		return xerrors.Errorf("Value in field t.Message was too long")
	}

	if err := cw.WriteMajorTypeHeader(cbg.MajTextString, uint64(len(t.Message))); err != nil {
		return err
	}
	if _, err := io.WriteString(w, string(t.Message)); err != nil {
		return err
	}

	// t.PaymentOwed (big.Int) (struct)
	if len("PaymentOwed") > cbg.MaxLength {
		return xerrors.Errorf("Value in field \"PaymentOwed\" was too long")
	}

	if err := cw.WriteMajorTypeHeader(cbg.MajTextString, uint64(len("PaymentOwed"))); err != nil {
		return err
	}
	if _, err := io.WriteString(w, string("PaymentOwed")); err != nil {
		return err
	}

	if err := t.PaymentOwed.MarshalCBOR(cw); err != nil {
		return err
	}
	return nil
}

func (t *DealResponse) UnmarshalCBOR(r io.Reader) (err error) {
	*t = DealResponse{}

	cr := cbg.NewCborReader(r)

	maj, extra, err := cr.ReadHeader()
	if err != nil {
		return err
	}
	defer func() {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
	}()

	if maj != cbg.MajMap {
		return fmt.Errorf("cbor input should be of type map")
	}

	if extra > cbg.MaxLength {
		return fmt.Errorf("DealResponse: map struct too large (%d)", extra)
	}

	var name string
	n := extra

	for i := uint64(0); i < n; i++ {

		{
			sval, err := cbg.ReadString(cr)
			if err != nil {
				return err
			}

			name = string(sval)
		}

		switch name {
		// t.ID (types.DealID) (uint64)
		case "ID":

			{

				maj, extra, err = cr.ReadHeader()
				if err != nil {
					return err
				}
				if maj != cbg.MajUnsignedInt {
					return fmt.Errorf("wrong type for uint64 field")
				}
				t.ID = DealID(extra)

			}
			// t.Status (types.DealStatus) (uint64)
		case "Status":

			{

				maj, extra, err = cr.ReadHeader()
				if err != nil {
					return err
				}
				if maj != cbg.MajUnsignedInt {
					return fmt.Errorf("wrong type for uint64 field")
				}
				t.Status = DealStatus(extra)

			}
			// t.Message (string) (string)
		case "Message":

			{
				sval, err := cbg.ReadString(cr)
				if err != nil {
					return err
				}

				t.Message = string(sval)
			}
			// t.PaymentOwed (big.Int) (struct)
		case "PaymentOwed":

			{

				if err := t.PaymentOwed.UnmarshalCBOR(cr); err != nil {
					return xerrors.Errorf("unmarshaling t.PaymentOwed: %w", err)
				}

			}

		default:
			// Field doesn't exist on this type, so ignore it
			cbg.ScanForLinks(r, func(cid.Cid) {})
		}
	}

	return nil
}
func (t *Params) MarshalCBOR(w io.Writer) error {
	if t == nil {
		_, err := w.Write(cbg.CborNull)
		return err
	}

	cw := cbg.NewCborWriter(w)

	if _, err := cw.Write([]byte{166}); err != nil {
		return err
	}

	// t.PieceCID (cid.Cid) (struct)
	if len("PieceCID") > cbg.MaxLength {
		return xerrors.Errorf("Value in field \"PieceCID\" was too long")
	}

	if err := cw.WriteMajorTypeHeader(cbg.MajTextString, uint64(len("PieceCID"))); err != nil {
		return err
	}
	if _, err := io.WriteString(w, string("PieceCID")); err != nil {
		return err
	}

	if t.PieceCID == nil {
		if _, err := cw.Write(cbg.CborNull); err != nil {
			return err
		}
	} else {
		if err := cbg.WriteCid(cw, *t.PieceCID); err != nil {
			return xerrors.Errorf("failed to write cid field t.PieceCID: %w", err)
		}
	}

	// t.Selector (types.CborGenCompatibleNode) (struct)
	if len("Selector") > cbg.MaxLength {
		return xerrors.Errorf("Value in field \"Selector\" was too long")
	}

	if err := cw.WriteMajorTypeHeader(cbg.MajTextString, uint64(len("Selector"))); err != nil {
		return err
	}
	if _, err := io.WriteString(w, string("Selector")); err != nil {
		return err
	}

	if err := t.Selector.MarshalCBOR(cw); err != nil {
		return err
	}

	// t.UnsealPrice (big.Int) (struct)
	if len("UnsealPrice") > cbg.MaxLength {
		return xerrors.Errorf("Value in field \"UnsealPrice\" was too long")
	}

	if err := cw.WriteMajorTypeHeader(cbg.MajTextString, uint64(len("UnsealPrice"))); err != nil {
		return err
	}
	if _, err := io.WriteString(w, string("UnsealPrice")); err != nil {
		return err
	}

	if err := t.UnsealPrice.MarshalCBOR(cw); err != nil {
		return err
	}

	// t.PricePerByte (big.Int) (struct)
	if len("PricePerByte") > cbg.MaxLength {
		return xerrors.Errorf("Value in field \"PricePerByte\" was too long")
	}

	if err := cw.WriteMajorTypeHeader(cbg.MajTextString, uint64(len("PricePerByte"))); err != nil {
		return err
	}
	if _, err := io.WriteString(w, string("PricePerByte")); err != nil {
		return err
	}

	if err := t.PricePerByte.MarshalCBOR(cw); err != nil {
		return err
	}

	// t.PaymentInterval (uint64) (uint64)
	if len("PaymentInterval") > cbg.MaxLength {
		return xerrors.Errorf("Value in field \"PaymentInterval\" was too long")
	}

	if err := cw.WriteMajorTypeHeader(cbg.MajTextString, uint64(len("PaymentInterval"))); err != nil {
		return err
	}
	if _, err := io.WriteString(w, string("PaymentInterval")); err != nil {
		return err
	}

	if err := cw.WriteMajorTypeHeader(cbg.MajUnsignedInt, uint64(t.PaymentInterval)); err != nil {
		return err
	}

	// t.PaymentIntervalIncrease (uint64) (uint64)
	if len("PaymentIntervalIncrease") > cbg.MaxLength {
		return xerrors.Errorf("Value in field \"PaymentIntervalIncrease\" was too long")
	}

	if err := cw.WriteMajorTypeHeader(cbg.MajTextString, uint64(len("PaymentIntervalIncrease"))); err != nil {
		return err
	}
	if _, err := io.WriteString(w, string("PaymentIntervalIncrease")); err != nil {
		return err
	}

	if err := cw.WriteMajorTypeHeader(cbg.MajUnsignedInt, uint64(t.PaymentIntervalIncrease)); err != nil {
		return err
	}

	return nil
}

func (t *Params) UnmarshalCBOR(r io.Reader) (err error) {
	*t = Params{}

	cr := cbg.NewCborReader(r)

	maj, extra, err := cr.ReadHeader()
	if err != nil {
		return err
	}
	defer func() {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
	}()

	if maj != cbg.MajMap {
		return fmt.Errorf("cbor input should be of type map")
	}

	if extra > cbg.MaxLength {
		return fmt.Errorf("Params: map struct too large (%d)", extra)
	}

	var name string
	n := extra

	for i := uint64(0); i < n; i++ {

		{
			sval, err := cbg.ReadString(cr)
			if err != nil {
				return err
			}

			name = string(sval)
		}

		switch name {
		// t.PieceCID (cid.Cid) (struct)
		case "PieceCID":

			{

				b, err := cr.ReadByte()
				if err != nil {
					return err
				}
				if b != cbg.CborNull[0] {
					if err := cr.UnreadByte(); err != nil {
						return err
					}

					c, err := cbg.ReadCid(cr)
					if err != nil {
						return xerrors.Errorf("failed to read cid field t.PieceCID: %w", err)
					}

					t.PieceCID = &c
				}

			}
			// t.Selector (types.CborGenCompatibleNode) (struct)
		case "Selector":

			{

				if err := t.Selector.UnmarshalCBOR(cr); err != nil {
					return xerrors.Errorf("unmarshaling t.Selector: %w", err)
				}

			}
			// t.UnsealPrice (big.Int) (struct)
		case "UnsealPrice":

			{

				if err := t.UnsealPrice.UnmarshalCBOR(cr); err != nil {
					return xerrors.Errorf("unmarshaling t.UnsealPrice: %w", err)
				}

			}
			// t.PricePerByte (big.Int) (struct)
		case "PricePerByte":

			{

				if err := t.PricePerByte.UnmarshalCBOR(cr); err != nil {
					return xerrors.Errorf("unmarshaling t.PricePerByte: %w", err)
				}

			}
			// t.PaymentInterval (uint64) (uint64)
		case "PaymentInterval":

			{

				maj, extra, err = cr.ReadHeader()
				if err != nil {
					return err
				}
				if maj != cbg.MajUnsignedInt {
					return fmt.Errorf("wrong type for uint64 field")
				}
				t.PaymentInterval = uint64(extra)

			}
			// t.PaymentIntervalIncrease (uint64) (uint64)
		case "PaymentIntervalIncrease":

			{

				maj, extra, err = cr.ReadHeader()
				if err != nil {
					return err
				}
				if maj != cbg.MajUnsignedInt {
					return fmt.Errorf("wrong type for uint64 field")
				}
				t.PaymentIntervalIncrease = uint64(extra)

			}

		default:
			// Field doesn't exist on this type, so ignore it
			cbg.ScanForLinks(r, func(cid.Cid) {})
		}
	}

	return nil
}
func (t *QueryParams) MarshalCBOR(w io.Writer) error {
	if t == nil {
		_, err := w.Write(cbg.CborNull)
		return err
	}

	cw := cbg.NewCborWriter(w)

	if _, err := cw.Write([]byte{161}); err != nil {
		return err
	}

	// t.PieceCID (cid.Cid) (struct)
	if len("PieceCID") > cbg.MaxLength {
		return xerrors.Errorf("Value in field \"PieceCID\" was too long")
	}

	if err := cw.WriteMajorTypeHeader(cbg.MajTextString, uint64(len("PieceCID"))); err != nil {
		return err
	}
	if _, err := io.WriteString(w, string("PieceCID")); err != nil {
		return err
	}

	if t.PieceCID == nil {
		if _, err := cw.Write(cbg.CborNull); err != nil {
			return err
		}
	} else {
		if err := cbg.WriteCid(cw, *t.PieceCID); err != nil {
			return xerrors.Errorf("failed to write cid field t.PieceCID: %w", err)
		}
	}

	return nil
}

func (t *QueryParams) UnmarshalCBOR(r io.Reader) (err error) {
	*t = QueryParams{}

	cr := cbg.NewCborReader(r)

	maj, extra, err := cr.ReadHeader()
	if err != nil {
		return err
	}
	defer func() {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
	}()

	if maj != cbg.MajMap {
		return fmt.Errorf("cbor input should be of type map")
	}

	if extra > cbg.MaxLength {
		return fmt.Errorf("QueryParams: map struct too large (%d)", extra)
	}

	var name string
	n := extra

	for i := uint64(0); i < n; i++ {

		{
			sval, err := cbg.ReadString(cr)
			if err != nil {
				return err
			}

			name = string(sval)
		}

		switch name {
		// t.PieceCID (cid.Cid) (struct)
		case "PieceCID":

			{

				b, err := cr.ReadByte()
				if err != nil {
					return err
				}
				if b != cbg.CborNull[0] {
					if err := cr.UnreadByte(); err != nil {
						return err
					}

					c, err := cbg.ReadCid(cr)
					if err != nil {
						return xerrors.Errorf("failed to read cid field t.PieceCID: %w", err)
					}

					t.PieceCID = &c
				}

			}

		default:
			// Field doesn't exist on this type, so ignore it
			cbg.ScanForLinks(r, func(cid.Cid) {})
		}
	}

	return nil
}
func (t *DealPayment) MarshalCBOR(w io.Writer) error {
	if t == nil {
		_, err := w.Write(cbg.CborNull)
		return err
	}

	cw := cbg.NewCborWriter(w)

	if _, err := cw.Write([]byte{163}); err != nil {
		return err
	}

	// t.ID (types.DealID) (uint64)
	if len("ID") > cbg.MaxLength {
		return xerrors.Errorf("Value in field \"ID\" was too long")
	}

	if err := cw.WriteMajorTypeHeader(cbg.MajTextString, uint64(len("ID"))); err != nil {
		return err
	}
	if _, err := io.WriteString(w, string("ID")); err != nil {
		return err
	}

	if err := cw.WriteMajorTypeHeader(cbg.MajUnsignedInt, uint64(t.ID)); err != nil {
		return err
	}

	// t.PaymentChannel (address.Address) (struct)
	if len("PaymentChannel") > cbg.MaxLength {
		return xerrors.Errorf("Value in field \"PaymentChannel\" was too long")
	}

	if err := cw.WriteMajorTypeHeader(cbg.MajTextString, uint64(len("PaymentChannel"))); err != nil {
		return err
	}
	if _, err := io.WriteString(w, string("PaymentChannel")); err != nil {
		return err
	}

	if err := t.PaymentChannel.MarshalCBOR(cw); err != nil {
		return err
	}

	// t.PaymentVoucher (paych.SignedVoucher) (struct)
	if len("PaymentVoucher") > cbg.MaxLength {
		return xerrors.Errorf("Value in field \"PaymentVoucher\" was too long")
	}

	if err := cw.WriteMajorTypeHeader(cbg.MajTextString, uint64(len("PaymentVoucher"))); err != nil {
		return err
	}
	if _, err := io.WriteString(w, string("PaymentVoucher")); err != nil {
		return err
	}

	if err := t.PaymentVoucher.MarshalCBOR(cw); err != nil {
		return err
	}
	return nil
}

func (t *DealPayment) UnmarshalCBOR(r io.Reader) (err error) {
	*t = DealPayment{}

	cr := cbg.NewCborReader(r)

	maj, extra, err := cr.ReadHeader()
	if err != nil {
		return err
	}
	defer func() {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
	}()

	if maj != cbg.MajMap {
		return fmt.Errorf("cbor input should be of type map")
	}

	if extra > cbg.MaxLength {
		return fmt.Errorf("DealPayment: map struct too large (%d)", extra)
	}

	var name string
	n := extra

	for i := uint64(0); i < n; i++ {

		{
			sval, err := cbg.ReadString(cr)
			if err != nil {
				return err
			}

			name = string(sval)
		}

		switch name {
		// t.ID (types.DealID) (uint64)
		case "ID":

			{

				maj, extra, err = cr.ReadHeader()
				if err != nil {
					return err
				}
				if maj != cbg.MajUnsignedInt {
					return fmt.Errorf("wrong type for uint64 field")
				}
				t.ID = DealID(extra)

			}
			// t.PaymentChannel (address.Address) (struct)
		case "PaymentChannel":

			{

				if err := t.PaymentChannel.UnmarshalCBOR(cr); err != nil {
					return xerrors.Errorf("unmarshaling t.PaymentChannel: %w", err)
				}

			}
			// t.PaymentVoucher (paych.SignedVoucher) (struct)
		case "PaymentVoucher":

			{

				b, err := cr.ReadByte()
				if err != nil {
					return err
				}
				if b != cbg.CborNull[0] {
					if err := cr.UnreadByte(); err != nil {
						return err
					}
					t.PaymentVoucher = new(paych.SignedVoucher)
					if err := t.PaymentVoucher.UnmarshalCBOR(cr); err != nil {
						return xerrors.Errorf("unmarshaling t.PaymentVoucher pointer: %w", err)
					}
				}

			}

		default:
			// Field doesn't exist on this type, so ignore it
			cbg.ScanForLinks(r, func(cid.Cid) {})
		}
	}

	return nil
}
