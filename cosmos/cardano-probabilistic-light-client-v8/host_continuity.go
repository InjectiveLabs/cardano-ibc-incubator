package probabilistic

import (
	"encoding/binary"

	state "github.com/cardano-foundation/cardano-ibc-incubator/cosmos/cardano-probabilistic-light-client-core/state"
	sdk "github.com/cosmos/cosmos-sdk/types"
)

var hostProgressKey = []byte("snapshots/v2/host/continuity")
var previousHostProgressKey = []byte("snapshots/v2/host/previousContinuity")

type hostProgressContextKey struct{}
type hostProgressProvider func(sdk.Context) *state.HostProgress

// HostLivenessBeginBlocker records host progress before the host's block hooks run.
func HostLivenessBeginBlocker(next sdk.BeginBlocker) sdk.BeginBlocker {
	return func(ctx sdk.Context) (sdk.BeginBlock, error) {
		store := ctx.KVStore(SharedStoreKey)
		previous := store.Get(hostProgressKey)
		progress, err := state.AdvanceHostProgress(state.Context{Time: ctx.BlockTime(), Height: ctx.BlockHeight()}, decodeHostProgress(previous))
		if err != nil {
			return sdk.BeginBlock{}, err
		}
		if len(previous) != 0 {
			store.Set(previousHostProgressKey, previous)
		}
		store.Set(hostProgressKey, encodeHostProgress(progress))
		return next(ctx)
	}
}

// HostLivenessDecorator snapshots host progress after gas setup and before IBC proof verification.
type HostLivenessDecorator struct{}

func (HostLivenessDecorator) AnteHandle(ctx sdk.Context, tx sdk.Tx, simulate bool, next sdk.AnteHandler) (sdk.Context, error) {
	store := ctx.KVStore(SharedStoreKey)
	current, previous := store.Get(hostProgressKey), store.Get(previousHostProgressKey)
	progress := decodeHostProgress(current)
	if progress == nil || progress.Height != ctx.BlockHeight() {
		progress = decodeHostProgress(previous)
	}
	// A simulation can keep an older context while the underlying store advances.
	provider := hostProgressProvider(func(sdk.Context) *state.HostProgress { return progress })
	return next(ctx.WithValue(hostProgressContextKey{}, provider), tx, simulate)
}

func encodeHostProgress(p *state.HostProgress) []byte {
	data := make([]byte, 48)
	binary.BigEndian.PutUint64(data, uint64(p.Height))
	binary.BigEndian.PutUint64(data[8:], p.TimeUnixNs)
	binary.BigEndian.PutUint64(data[16:], p.PreviousTimeUnixNs)
	binary.BigEndian.PutUint64(data[24:], p.ClockID)
	binary.BigEndian.PutUint64(data[32:], p.ObservationTime)
	binary.BigEndian.PutUint64(data[40:], p.PreviousObservationTime)
	return data
}
func decodeHostProgress(data []byte) *state.HostProgress {
	if len(data) != 48 {
		return nil
	}
	return &state.HostProgress{Height: int64(binary.BigEndian.Uint64(data)), TimeUnixNs: binary.BigEndian.Uint64(data[8:]), PreviousTimeUnixNs: binary.BigEndian.Uint64(data[16:]), ClockID: binary.BigEndian.Uint64(data[24:]), ObservationTime: binary.BigEndian.Uint64(data[32:]), PreviousObservationTime: binary.BigEndian.Uint64(data[40:])}
}
func hostProgressEvidence(ctx sdk.Context) *state.HostProgress {
	if provider, ok := ctx.Value(hostProgressContextKey{}).(hostProgressProvider); ok {
		return provider(ctx)
	}
	// Queries read the clock at their committed height. Transactions use the
	// immutable ante snapshot above. The module exports both clock records.
	return decodeHostProgress(ctx.KVStore(SharedStoreKey).Get(hostProgressKey))
}
