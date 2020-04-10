package volumewatcher

import (
	"fmt"
	"testing"

	cstructs "github.com/hashicorp/nomad/client/structs"
	"github.com/hashicorp/nomad/nomad/mock"
	"github.com/hashicorp/nomad/nomad/state"
	"github.com/hashicorp/nomad/nomad/structs"
	"github.com/stretchr/testify/require"
)

// TestVolumeWatch_OneReap tests one pass through the reaper
func TestVolumeWatch_OneReap(t *testing.T) {
	t.Parallel()
	require := require.New(t)

	cases := []struct {
		Name                          string
		Volume                        *structs.CSIVolume
		Node                          *structs.Node
		ControllerRequired            bool
		ExpectedErr                   string
		ExpectedClaimsCount           int
		ExpectedNodeDetachCount       int
		ExpectedControllerDetachCount int
		ExpectedUpdateClaimsCount     int
		srv                           *MockRPCServer
	}{
		{
			Name:               "No terminal allocs",
			Volume:             mock.CSIVolume(mock.CSIPlugin()),
			ControllerRequired: true,
			srv: &MockRPCServer{
				state:                  state.TestStateStore(t),
				nextCSINodeDetachError: fmt.Errorf("should never see this"),
			},
		},
		{
			Name:                    "NodeDetachVolume fails",
			ControllerRequired:      true,
			ExpectedErr:             "some node plugin error",
			ExpectedNodeDetachCount: 1,
			srv: &MockRPCServer{
				state:                  state.TestStateStore(t),
				nextCSINodeDetachError: fmt.Errorf("some node plugin error"),
			},
		},
		{
			Name:                      "NodeDetachVolume node-only happy path",
			ControllerRequired:        false,
			ExpectedNodeDetachCount:   1,
			ExpectedUpdateClaimsCount: 3,
			srv: &MockRPCServer{
				state: state.TestStateStore(t),
			},
		},
		{
			Name:                      "ControllerDetachVolume no controllers available",
			Node:                      mock.Node(),
			ControllerRequired:        true,
			ExpectedErr:               "Unknown node",
			ExpectedNodeDetachCount:   1,
			ExpectedUpdateClaimsCount: 1,
			srv: &MockRPCServer{
				state: state.TestStateStore(t),
			},
		},
		{
			Name:                          "ControllerDetachVolume controller error",
			ControllerRequired:            true,
			ExpectedErr:                   "some controller error",
			ExpectedNodeDetachCount:       1,
			ExpectedControllerDetachCount: 1,
			ExpectedUpdateClaimsCount:     1,
			srv: &MockRPCServer{
				state:                        state.TestStateStore(t),
				nextCSIControllerDetachError: fmt.Errorf("some controller error"),
			},
		},
		{
			Name:                          "ControllerDetachVolume happy path",
			ControllerRequired:            true,
			ExpectedNodeDetachCount:       1,
			ExpectedControllerDetachCount: 1,
			ExpectedUpdateClaimsCount:     3,
			srv: &MockRPCServer{
				state: state.TestStateStore(t),
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.Name, func(t *testing.T) {

			plugin := mock.CSIPlugin()
			plugin.ControllerRequired = tc.ControllerRequired
			node := testNode(tc.Node, plugin, tc.srv.State())
			alloc := mock.Alloc()
			vol := testVolume(tc.Volume, plugin, alloc.ID, node.ID)

			w := &volumeWatcher{
				v:            vol,
				rpc:          tc.srv,
				state:        tc.srv.State(),
				updateClaims: tc.srv.UpdateClaims,
			}

			err := w.volumeReapImpl(vol)
			if tc.ExpectedErr != "" {
				require.Error(err, fmt.Sprintf("expected: %q", tc.ExpectedErr))
				require.Contains(err.Error(), tc.ExpectedErr)
			} else {
				require.NoError(err)
			}
			require.Equal(tc.ExpectedNodeDetachCount,
				tc.srv.countCSINodeDetachVolume, "node detach RPC count")
			require.Equal(tc.ExpectedControllerDetachCount,
				tc.srv.countCSIControllerDetachVolume, "controller detach RPC count")
			require.Equal(tc.ExpectedUpdateClaimsCount,
				tc.srv.countUpdateClaims, "update claims count")
		})
	}
}

// TestVolumeWatch_OneReap tests multiple passes through the reaper,
// updating state after each one
func TestVolumeWatch_ReapStates(t *testing.T) {
	t.Parallel()
	require := require.New(t)

	srv := &MockRPCServer{state: state.TestStateStore(t)}
	plugin := mock.CSIPlugin()
	node := testNode(nil, plugin, srv.State())
	alloc := mock.Alloc()
	vol := testVolume(nil, plugin, alloc.ID, node.ID)

	w := &volumeWatcher{
		v:            vol,
		rpc:          srv,
		state:        srv.State(),
		updateClaims: srv.UpdateClaims,
	}

	srv.nextCSINodeDetachError = fmt.Errorf("some node plugin error")
	err := w.volumeReapImpl(vol)
	require.Error(err)
	require.Equal(vol.PastClaims[alloc.ID].State, structs.CSIVolumeClaimStateTaken)
	require.Equal(srv.countCSINodeDetachVolume, 1)
	require.Equal(srv.countCSIControllerDetachVolume, 0)
	require.Equal(srv.countUpdateClaims, 0)

	srv.nextCSINodeDetachError = nil
	srv.nextCSIControllerDetachError = fmt.Errorf("some controller plugin error")
	err = w.volumeReapImpl(vol)
	require.Error(err)
	require.Equal(vol.PastClaims[alloc.ID].State, structs.CSIVolumeClaimStateNodeDetached)
	require.Equal(srv.countUpdateClaims, 1)

	srv.nextCSIControllerDetachError = nil
	err = w.volumeReapImpl(vol)
	require.NoError(err)
	require.Equal(vol.PastClaims[alloc.ID].State, structs.CSIVolumeClaimStateFreed)
	require.Equal(srv.countUpdateClaims, 3)
}

// TestVolumeWatch_Batcher tests the update batching logic
func TestVolumeWatch_Batcher(t *testing.T) { t.Parallel() }

// TestVolumeWatch_Batcher tests the watcher registration logic that needs
// to happen during leader step-up/step-down
func TestVolumeWatch_EnableDisable(t *testing.T) { t.Parallel() }

// Create a client node with plugin info
func testNode(node *structs.Node, plugin *structs.CSIPlugin, s *state.StateStore) *structs.Node {
	if node != nil {
		return node
	}
	node = mock.Node()
	node.Attributes["nomad.version"] = "0.11.0" // client RPCs not supported on early version
	node.CSINodePlugins = map[string]*structs.CSIInfo{
		plugin.ID: {
			PluginID:                 plugin.ID,
			Healthy:                  true,
			RequiresControllerPlugin: plugin.ControllerRequired,
			NodeInfo:                 &structs.CSINodeInfo{},
		},
	}
	if plugin.ControllerRequired {
		node.CSIControllerPlugins = map[string]*structs.CSIInfo{
			plugin.ID: {
				PluginID:                 plugin.ID,
				Healthy:                  true,
				RequiresControllerPlugin: true,
				ControllerInfo: &structs.CSIControllerInfo{
					SupportsReadOnlyAttach:           true,
					SupportsAttachDetach:             true,
					SupportsListVolumes:              true,
					SupportsListVolumesAttachedNodes: false,
				},
			},
		}
	} else {
		node.CSIControllerPlugins = map[string]*structs.CSIInfo{}
	}
	s.UpsertNode(99, node)
	return node
}

// Create a test volume with claim info
func testVolume(vol *structs.CSIVolume, plugin *structs.CSIPlugin, allocID, nodeID string) *structs.CSIVolume {
	if vol != nil {
		return vol
	}
	vol = mock.CSIVolume(plugin)
	vol.ControllerRequired = plugin.ControllerRequired
	vol.ReadAllocs = map[string]*structs.Allocation{allocID: nil}
	vol.ReadClaims = map[string]*structs.CSIVolumeClaim{
		allocID: {
			AllocationID: allocID,
			NodeID:       nodeID,
			Mode:         structs.CSIVolumeClaimRead,
			State:        structs.CSIVolumeClaimStateTaken,
		},
	}
	return vol
}

type MockRPCServer struct {
	state *state.StateStore

	// mock responses for ClientCSI.NodeDetachVolume
	nextCSINodeDetachResponse *cstructs.ClientCSINodeDetachVolumeResponse
	nextCSINodeDetachError    error
	countCSINodeDetachVolume  int

	// mock responses for ClientCSI.ControllerDetachVolume
	nextCSIControllerDetachVolumeResponse *cstructs.ClientCSIControllerDetachVolumeResponse
	nextCSIControllerDetachError          error
	countCSIControllerDetachVolume        int

	countUpdateClaims int
}

func (srv *MockRPCServer) ControllerDetachVolume(args *cstructs.ClientCSIControllerDetachVolumeRequest, reply *cstructs.ClientCSIControllerDetachVolumeResponse) error {
	reply = srv.nextCSIControllerDetachVolumeResponse
	srv.countCSIControllerDetachVolume++
	return srv.nextCSIControllerDetachError
}

func (srv *MockRPCServer) NodeDetachVolume(args *cstructs.ClientCSINodeDetachVolumeRequest, reply *cstructs.ClientCSINodeDetachVolumeResponse) error {
	reply = srv.nextCSINodeDetachResponse
	srv.countCSINodeDetachVolume++
	return srv.nextCSINodeDetachError

}

func (srv *MockRPCServer) State() *state.StateStore { return srv.state }

func (srv *MockRPCServer) UpdateClaims(claims []structs.CSIVolumeClaimRequest) (uint64, error) {
	srv.countUpdateClaims++
	return 0, nil
}
