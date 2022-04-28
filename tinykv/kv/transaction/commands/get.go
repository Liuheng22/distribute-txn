package commands

import (
	"encoding/hex"

	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

type Get struct {
	ReadOnly
	CommandBase
	request *kvrpcpb.GetRequest
}

func NewGet(request *kvrpcpb.GetRequest) Get {
	return Get{
		CommandBase: CommandBase{
			context: request.Context,
			startTs: request.Version,
		},
		request: request,
	}
}

func (g *Get) Read(txn *mvcc.RoTxn) (interface{}, [][]byte, error) {
	key := g.request.Key
	log.Debug("read key", zap.Uint64("start_ts", txn.StartTS),
		zap.String("key", hex.EncodeToString(key)))
	response := new(kvrpcpb.GetResponse)

	// YOUR CODE HERE (lab1).
	// Check for locks and their visibilities.
	// Hint: Check the interfaces provided by `mvcc.RoTxn`.
	// 读取数据
	// 检查锁，看是否当前时间之前还有未提交的事务
	lock, err := txn.GetLock(key)
	if err != nil {
		// 出错了
		return nil, nil, err
	}
	if lock != nil && lock.IsLockedFor(key, txn.StartTS, response) {
		// 有锁且在startts之前
		// 没出错，只是key没找到
		return response, nil, nil
	}
	// YOUR CODE HERE (lab1).
	// Search writes for a committed value, set results in the response.
	// Hint: Check the interfaces provided by `mvcc.RoTxn`.
	value, err := txn.GetValue(key)
	if err != nil {
		return nil, nil, err
	}
	if value == nil {
		response.NotFound = true
	} else {
		response.Value = value
	}
	return response, nil, nil
}
