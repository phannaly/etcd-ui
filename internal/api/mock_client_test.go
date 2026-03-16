package api

import (
	"context"
	"fmt"
	"time"

	"github.com/phannaly/etcd-ui/internal/etcd"
)

type mockClient struct {
	clusterInfoFn          func(ctx context.Context) *etcd.ClusterInfo
	listAllFn              func(ctx context.Context) ([]*etcd.KeyNode, error)
	listPrefixFn           func(ctx context.Context, prefix string) ([]*etcd.KeyNode, error)
	getKeyFn               func(ctx context.Context, key string) (*etcd.KeyNode, error)
	searchFn               func(ctx context.Context, query string) ([]*etcd.KeyNode, error)
	putFn                  func(ctx context.Context, key, value string) error
	putWithTTLFn           func(ctx context.Context, key, value string, ttl int64) error
	deleteFn               func(ctx context.Context, key string) (int64, error)
	deletePrefixFn         func(ctx context.Context, prefix string) (int64, error)
	watchFn                func(ctx context.Context, prefix string, out chan<- etcd.WatchEvent)
	keyHistoryFn           func(ctx context.Context, key string, limit int) ([]etcd.HistoryEntry, error)
	leaseListFn            func(ctx context.Context) ([]etcd.LeaseInfo, error)
	leaseRevokeFn          func(ctx context.Context, id int64) error
	exportFn               func(ctx context.Context, prefix string) ([]etcd.ExportEntry, error)
	importFn               func(ctx context.Context, entries []etcd.ExportEntry) (int, error)
	authStatusFn           func(ctx context.Context) (*etcd.AuthStatus, error)
	authEnableFn           func(ctx context.Context) error
	authDisableFn          func(ctx context.Context) error
	userListFn             func(ctx context.Context) ([]etcd.UserInfo, error)
	userAddFn              func(ctx context.Context, name, password string) error
	userDeleteFn           func(ctx context.Context, name string) error
	userChangePasswordFn   func(ctx context.Context, name, password string) error
	userGrantRoleFn        func(ctx context.Context, user, role string) error
	userRevokeRoleFn       func(ctx context.Context, user, role string) error
	roleListFn             func(ctx context.Context) ([]etcd.RoleInfo, error)
	roleAddFn              func(ctx context.Context, name string) error
	roleDeleteFn           func(ctx context.Context, name string) error
	roleGrantPermissionFn  func(ctx context.Context, role, key, rangeEnd, permType string) error
	roleRevokePermissionFn func(ctx context.Context, role, key, rangeEnd string) error
	maxKeysFn              func() int
	pingFn                 func(ctx context.Context) bool
}

func (m *mockClient) ClusterInfo(ctx context.Context) *etcd.ClusterInfo {
	if m.clusterInfoFn != nil {
		return m.clusterInfoFn(ctx)
	}
	return &etcd.ClusterInfo{Connected: true, Endpoints: []string{"localhost:2379"}}
}

func (m *mockClient) ListAll(ctx context.Context) ([]*etcd.KeyNode, error) {
	if m.listAllFn != nil {
		return m.listAllFn(ctx)
	}
	return nil, nil
}

func (m *mockClient) ListPrefix(ctx context.Context, prefix string) ([]*etcd.KeyNode, error) {
	if m.listPrefixFn != nil {
		return m.listPrefixFn(ctx, prefix)
	}
	return nil, nil
}

func (m *mockClient) GetKey(ctx context.Context, key string) (*etcd.KeyNode, error) {
	if m.getKeyFn != nil {
		return m.getKeyFn(ctx, key)
	}
	return nil, fmt.Errorf("key not found: %s", key)
}

func (m *mockClient) Search(ctx context.Context, query string) ([]*etcd.KeyNode, error) {
	if m.searchFn != nil {
		return m.searchFn(ctx, query)
	}
	return nil, nil
}

func (m *mockClient) Put(ctx context.Context, key, value string) error {
	if m.putFn != nil {
		return m.putFn(ctx, key, value)
	}
	return nil
}

func (m *mockClient) PutWithTTL(ctx context.Context, key, value string, ttl int64) error {
	if m.putWithTTLFn != nil {
		return m.putWithTTLFn(ctx, key, value, ttl)
	}
	return nil
}

func (m *mockClient) Delete(ctx context.Context, key string) (int64, error) {
	if m.deleteFn != nil {
		return m.deleteFn(ctx, key)
	}
	return 1, nil
}

func (m *mockClient) DeletePrefix(ctx context.Context, prefix string) (int64, error) {
	if m.deletePrefixFn != nil {
		return m.deletePrefixFn(ctx, prefix)
	}
	return 0, nil
}

func (m *mockClient) Watch(ctx context.Context, prefix string, out chan<- etcd.WatchEvent) {
	if m.watchFn != nil {
		m.watchFn(ctx, prefix, out)
	}
}

func (m *mockClient) KeyHistory(ctx context.Context, key string, limit int) ([]etcd.HistoryEntry, error) {
	if m.keyHistoryFn != nil {
		return m.keyHistoryFn(ctx, key, limit)
	}
	return nil, nil
}

func (m *mockClient) LeaseList(ctx context.Context) ([]etcd.LeaseInfo, error) {
	if m.leaseListFn != nil {
		return m.leaseListFn(ctx)
	}
	return nil, nil
}

func (m *mockClient) LeaseRevoke(ctx context.Context, id int64) error {
	if m.leaseRevokeFn != nil {
		return m.leaseRevokeFn(ctx, id)
	}
	return nil
}

func (m *mockClient) Export(ctx context.Context, prefix string) ([]etcd.ExportEntry, error) {
	if m.exportFn != nil {
		return m.exportFn(ctx, prefix)
	}
	return nil, nil
}

func (m *mockClient) Import(ctx context.Context, entries []etcd.ExportEntry) (int, error) {
	if m.importFn != nil {
		return m.importFn(ctx, entries)
	}
	return len(entries), nil
}

func (m *mockClient) AuthStatus(ctx context.Context) (*etcd.AuthStatus, error) {
	if m.authStatusFn != nil {
		return m.authStatusFn(ctx)
	}
	return &etcd.AuthStatus{Enabled: false}, nil
}

func (m *mockClient) AuthEnable(ctx context.Context) error {
	if m.authEnableFn != nil {
		return m.authEnableFn(ctx)
	}
	return nil
}

func (m *mockClient) AuthDisable(ctx context.Context) error {
	if m.authDisableFn != nil {
		return m.authDisableFn(ctx)
	}
	return nil
}

func (m *mockClient) UserList(ctx context.Context) ([]etcd.UserInfo, error) {
	if m.userListFn != nil {
		return m.userListFn(ctx)
	}
	return nil, nil
}

func (m *mockClient) UserAdd(ctx context.Context, name, password string) error {
	if m.userAddFn != nil {
		return m.userAddFn(ctx, name, password)
	}
	return nil
}

func (m *mockClient) UserDelete(ctx context.Context, name string) error {
	if m.userDeleteFn != nil {
		return m.userDeleteFn(ctx, name)
	}
	return nil
}

func (m *mockClient) UserChangePassword(ctx context.Context, name, password string) error {
	if m.userChangePasswordFn != nil {
		return m.userChangePasswordFn(ctx, name, password)
	}
	return nil
}

func (m *mockClient) UserGrantRole(ctx context.Context, user, role string) error {
	if m.userGrantRoleFn != nil {
		return m.userGrantRoleFn(ctx, user, role)
	}
	return nil
}

func (m *mockClient) UserRevokeRole(ctx context.Context, user, role string) error {
	if m.userRevokeRoleFn != nil {
		return m.userRevokeRoleFn(ctx, user, role)
	}
	return nil
}

func (m *mockClient) RoleList(ctx context.Context) ([]etcd.RoleInfo, error) {
	if m.roleListFn != nil {
		return m.roleListFn(ctx)
	}
	return nil, nil
}

func (m *mockClient) RoleAdd(ctx context.Context, name string) error {
	if m.roleAddFn != nil {
		return m.roleAddFn(ctx, name)
	}
	return nil
}

func (m *mockClient) RoleDelete(ctx context.Context, name string) error {
	if m.roleDeleteFn != nil {
		return m.roleDeleteFn(ctx, name)
	}
	return nil
}

func (m *mockClient) RoleGrantPermission(ctx context.Context, role, key, rangeEnd, permType string) error {
	if m.roleGrantPermissionFn != nil {
		return m.roleGrantPermissionFn(ctx, role, key, rangeEnd, permType)
	}
	return nil
}

func (m *mockClient) RoleRevokePermission(ctx context.Context, role, key, rangeEnd string) error {
	if m.roleRevokePermissionFn != nil {
		return m.roleRevokePermissionFn(ctx, role, key, rangeEnd)
	}
	return nil
}

func (m *mockClient) MaxKeys() int {
	if m.maxKeysFn != nil {
		return m.maxKeysFn()
	}
	return 5000
}

func (m *mockClient) Ping(ctx context.Context) bool {
	if m.pingFn != nil {
		return m.pingFn(ctx)
	}
	return true
}

type nopAudit struct{ entries []string }

func (n *nopAudit) Log(action, key, old, new, user, ip, detail string) {
	n.entries = append(n.entries, action+":"+key)
}

func newTestHandler(client *mockClient, readOnly bool) (*Handler, *nopAudit) {
	alog := &nopAudit{}
	h := &Handler{
		client:   client,
		alog:     alog,
		readOnly: readOnly,
		watchers: make(map[string]chan etcd.WatchEvent),
		ratemap:  make(map[string][]time.Time),
	}
	return h, alog
}
