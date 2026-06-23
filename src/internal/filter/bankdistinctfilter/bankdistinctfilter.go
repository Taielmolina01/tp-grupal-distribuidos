package bankdistinctfilter

import (
	"tp-grupal-distribuidos/internal/common/account"
	"tp-grupal-distribuidos/internal/common/filter"
	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/records"
	"tp-grupal-distribuidos/internal/common/normalizer"
	"tp-grupal-distribuidos/internal/common/worker"
	"tp-grupal-distribuidos/internal/filter/commondistinctfilter"
)

func CreateBankDistinctFilter(config filter.FilterConfig) (worker.Worker, error) {
	return commondistinctfilter.NewDistinctFilter(
		config,
		func(ac account.Account) string {
			return normalizer.NormalizeBankID(ac.BankId)
		},
		records.AccountCodec,
	)
}
