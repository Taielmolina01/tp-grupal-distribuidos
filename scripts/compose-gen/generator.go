package main

import (
	"fmt"
	"strings"
)

func writeMaxBatchConfig(b *strings.Builder, cfg *Config) {
	b.WriteString("      - MAX_BATCH_SIZE=1000\n")
	fmt.Fprintf(b, "      - MAX_BATCH_KB=%d\n", cfg.MaxBatchKB)
}

func generateCompose(cfg *Config) string {
	var b strings.Builder

	b.WriteString("services:\n")

	writeRabbitmq(&b)
	writeGateway(&b, cfg)
	writeClients(&b, cfg)

	b.WriteString("\n  # Query 1\n\n")
	writeFilterCurrency(&b, cfg)
	writeFilterAmount(&b, cfg)

	b.WriteString("\n  # Query 2\n\n")
	writeReducerQ2(&b, cfg)
	writeFilterBankIdAlreadySeen(&b, cfg)
	writeJoinQ2(&b, cfg)

	b.WriteString("\n  # Query 4\n\n")
	writeFilterAndSplitterQ4(&b, cfg)
	writeJoinAccountsQ4(&b, cfg)
	writeAcumAccountsQ4(&b, cfg)
	writeFilterAccountSeenQ4(&b, cfg)

	b.WriteString("\n  # Query 3\n\n")
	writeFilterRangeQ3(&b, cfg)
	writeSumQ3(&b, cfg)
	writeAggregateQ3(&b, cfg)
	writeAverageFilterQ3(&b, cfg)

	b.WriteString("\n  # Query 5\n\n")
	writeFilterDateAndPayment(&b, cfg)
	writeFetcher(&b, cfg)
	writeFilterAmtQ5(&b, cfg)
	writeCountReducerQ5(&b, cfg)

	b.WriteString("\n  # Tolerancia a fallas\n\n")
	writeWatchdogs(&b, cfg)

	return b.String()
}

func workerNodes(cfg *Config) []string {
	var nodes []string
	add := func(prefix string, n int) {
		for i := range n {
			nodes = append(nodes, fmt.Sprintf("%s_%d", prefix, i))
		}
	}

	nodes = append(nodes, "gateway")

	add("q1234_filter_currency", cfg.FilterCurrency)
	add("q1_filter_amount", cfg.FilterAmount)
	add("q2_reducer", cfg.ReducerQ2)
	add("q2_filter_bank_distinct", cfg.FilterBankIdAlreadySeen)
	add("q2_join", cfg.JoinQ2)
	add("q4_filter_and_splitter", cfg.FilterAndSplitterQ4)
	add("q4_join_accounts", cfg.JoinAccountsQ4)
	add("q4_acum_accounts", cfg.AcumAccountsQ4)
	add("q4_filter_account_seen", cfg.FilterAccountSeenQ4)
	add("q3_filter_range", cfg.FilterRangeQ3)
	add("q3_sum", cfg.SumQ3)
	add("q3_aggregate", cfg.AggregateQ3)
	add("q3_average_filter", cfg.AverageFilterQ3)
	add("q5_filter_date_and_payment", cfg.FilterDateAndPayment)
	add("q5_filter_amount", cfg.FilterAmtQ5)
	add("watchdog", cfg.Watchdogs)
	nodes = append(nodes, "q5_fetcher", "q5_count_reducer")

	return nodes
}

func writeWatchdogs(b *strings.Builder, cfg *Config) {
	nodes := strings.Join(workerNodes(cfg), " ")

	for i := range cfg.Watchdogs {
		ids := []int{}
		for j := range cfg.Watchdogs {
			if j > i {
				ids = append(ids, j)
			}
		}
		fmt.Fprintf(b, "  watchdog_%d:\n", i)
		b.WriteString("    build:\n")
		b.WriteString("      context: ./src/\n")
		b.WriteString("      dockerfile: cmd/watchdog/Dockerfile\n")
		fmt.Fprintf(b, "    container_name: watchdog_%d\n", i)
		rabbitmqDepends(b)
		watchdogDepends(b, ids)
		b.WriteString("    environment:\n")
		fmt.Fprintf(b, "      - ID=%d\n", i)
		fmt.Fprintf(b, "      - AMOUNT=%d\n", cfg.Watchdogs)
		fmt.Fprintf(b, "      - NODES=%s\n", nodes)
		b.WriteString("      - INTERVAL=1s\n")
		b.WriteString("      - TIMEOUT=1s\n")
		b.WriteString("      - MAX_RETRIES=3\n")
		b.WriteString("      - STARTUP=5s\n")
		b.WriteString("      - HEALTH_PORT=8001\n")
		b.WriteString("      - BULLY_PORT=8002\n")
		b.WriteString("    volumes:\n")
		b.WriteString("      - /var/run/docker.sock:/var/run/docker.sock\n")
		jsonFileLogging(b)
		b.WriteString("\n")
	}
}

func queues(prefix string, n int) string {
	parts := make([]string, n)
	for i := range n {
		parts[i] = fmt.Sprintf("%s_%d", prefix, i)
	}
	return strings.Join(parts, ",")
}

func jsonFileLogging(b *strings.Builder) {
	b.WriteString("    logging:\n")
	b.WriteString("      driver: json-file\n")
	b.WriteString("      options:\n")
	b.WriteString("        max-size: \"50m\"\n")
	b.WriteString("        max-file: \"3\"\n")
}

func rabbitmqDepends(b *strings.Builder) {
	b.WriteString("    depends_on:\n")
	b.WriteString("      rabbitmq:\n")
	b.WriteString("        condition: service_healthy\n")
}

func watchdogDepends(b *strings.Builder, watchdogs []int) {
	for _, node := range watchdogs {
		fmt.Fprintf(b, "      watchdog_%d:\n", node)
		b.WriteString("        condition: service_started\n")
	}
}

func writeRabbitmq(b *strings.Builder) {
	b.WriteString("  rabbitmq:\n")
	b.WriteString("    build:\n")
	b.WriteString("      context: ./src/cmd/rabbitmq\n")
	b.WriteString("      dockerfile: Dockerfile\n")
	b.WriteString("    container_name: rabbitmq\n")
	b.WriteString("    environment:\n")
	b.WriteString("      - RABBITMQ_LOG_LEVELS=error\n")
	b.WriteString("      - RABBITMQ_SERVER_ADDITIONAL_ERL_ARGS=-rabbit vm_memory_high_watermark 0.4\n")
	b.WriteString("    healthcheck:\n")
	b.WriteString("      interval: 5s\n")
	b.WriteString("      retries: 10\n")
	b.WriteString("      start_period: 50s\n")
	b.WriteString("      test: rabbitmq-diagnostics check_port_connectivity\n")
	b.WriteString("      timeout: 3s\n")
	b.WriteString("    ports:\n")
	b.WriteString("      - 5672:5672\n")
	b.WriteString("      - 15672:15672\n")
	b.WriteString("    logging:\n")
	b.WriteString("      driver: \"none\"\n")
	b.WriteString("\n")
}

func writeGateway(b *strings.Builder, cfg *Config) {
	queryEofs := fmt.Sprintf("1:%d,2:%d,3:%d,4:%d,5:1", cfg.FilterAmount, cfg.JoinQ2, cfg.AverageFilterQ3, cfg.FilterAccountSeenQ4)

	b.WriteString("  gateway:\n")
	b.WriteString("    build:\n")
	b.WriteString("      context: ./src/\n")
	b.WriteString("      dockerfile: cmd/gateway/Dockerfile\n")
	b.WriteString("    container_name: gateway\n")
	rabbitmqDepends(b)
	b.WriteString("    environment:\n")
	b.WriteString("      - ACCOUNTS_CLUSTER_PREFIX=Q2_accounts\n")
	fmt.Fprintf(b, "      - ACCOUNTS_CLUSTER_AMOUNT=%d\n", cfg.FilterBankIdAlreadySeen)
	transfersClusters := fmt.Sprintf("Q1234_filter_currency:%d,Q5_filter:%d",
		cfg.FilterCurrency, cfg.FilterDateAndPayment)
	fmt.Fprintf(b, "      - TRANSFERS_CLUSTERS=%s\n", transfersClusters)
	b.WriteString("      - RESULTS_QUEUE=results_queue\n")
	b.WriteString("      - MOM_HOST=rabbitmq\n")
	b.WriteString("      - MOM_PORT=5672\n")
	b.WriteString("      - SEQ_CHECKPOINT_EVERY=100\n")
	b.WriteString("      - SERVER_HOST=gateway\n")
	b.WriteString("      - SERVER_PORT=5678\n")
	fmt.Fprintf(b, "      - QUERY_EOFS_EXPECTED=%s\n", queryEofs)
	b.WriteString("\n")
}

func writeFetcher(b *strings.Builder, cfg *Config) {
	b.WriteString("  q5_fetcher:\n")
	b.WriteString("    build:\n")
	b.WriteString("      context: ./src/\n")
	b.WriteString("      dockerfile: cmd/fetcher/Dockerfile\n")
	b.WriteString("    container_name: q5_fetcher\n")
	rabbitmqDepends(b)
	b.WriteString("    environment:\n")
	b.WriteString("      - MOM_HOST=rabbitmq\n")
	b.WriteString("      - MOM_PORT=5672\n")
	b.WriteString("      - INPUT_QUEUE=Q5_filtered_fetcher_q\n")
	b.WriteString("      - INPUT_EXCHANGE=Q5_filtered\n")
	b.WriteString("      - INPUT_ROUTING_KEYS=shard-0\n")
	b.WriteString("      - OUTPUT_CLUSTERS=Q5_fetcher_output:2\n")
	fmt.Fprintf(b, "      - INPUT_SENDERS=%d\n", cfg.FilterDateAndPayment)
	b.WriteString("      - QUERY_ID=5\n")
	b.WriteString("      - QUOTE=USD\n")
	b.WriteString("\n")
}

func writeCountReducerQ5(b *strings.Builder, cfg *Config) {
	b.WriteString("  q5_count_reducer:\n")
	b.WriteString("    build:\n")
	b.WriteString("      context: ./src/\n")
	b.WriteString("      dockerfile: cmd/reducer/Dockerfile\n")
	b.WriteString("    container_name: q5_count_reducer\n")
	rabbitmqDepends(b)
	b.WriteString("    environment:\n")
	b.WriteString("      - MOM_HOST=rabbitmq\n")
	b.WriteString("      - MOM_PORT=5672\n")
	b.WriteString("      - ID=0\n")
	b.WriteString("      - REDUCER_AMOUNT=1\n")
	fmt.Fprintf(b, "      - INPUT_EOFS_EXPECTED=%d\n", cfg.FilterAmtQ5)
	b.WriteString("      - INPUT_QUEUE=Q5_filtered_to_count_q\n")
	b.WriteString("      - OUTPUT_QUEUES=results_queue\n")
	b.WriteString("      - REDUCER_TYPE=COUNT\n")
	b.WriteString("      - QUERY_ID=5\n")
	b.WriteString("      - PERSIST_PATH=/var/bkp/q5_count_reducer\n")
	b.WriteString("      - PERSIST_BATCH_SIZE=50\n")
	b.WriteString("      - PERSIST_FLUSH_INTERVAL=1s\n")
	b.WriteString("\n")
}

func writeClients(b *strings.Builder, cfg *Config) {
	for i := range cfg.Clients {
		fmt.Fprintf(b, "  client_%d:\n", i)
		b.WriteString("    build:\n")
		b.WriteString("      context: ./src/\n")
		b.WriteString("      dockerfile: cmd/client/Dockerfile\n")
		fmt.Fprintf(b, "    container_name: client_%d\n", i)
		b.WriteString("    depends_on:\n")
		b.WriteString("      - gateway\n")
		b.WriteString("    environment:\n")
		b.WriteString("      - SERVER_HOST=gateway\n")
		b.WriteString("      - SERVER_PORT=5678\n")
		fmt.Fprintf(b, "      - INPUT_FILE_ACCOUNTS=/input/accounts_%d.csv\n", i)
		fmt.Fprintf(b, "      - INPUT_FILE_TRANS=/input/transactions_%d.csv\n", i)
		fmt.Fprintf(b, "      - OUTPUT_FILE_PREFIX=/output/output_%d\n", i)
		b.WriteString("      - MAX_BATCH_SIZE=1000\n")
		fmt.Fprintf(b, "      - MAX_BATCH_KB=%d\n", cfg.MaxBatchKB)
		b.WriteString("    volumes:\n")
		b.WriteString("      - ./input:/input\n")
		b.WriteString("      - ./output:/output\n")
		b.WriteString("\n")
	}
}

func writeFilterCurrency(b *strings.Builder, cfg *Config) {
	outputClusters := fmt.Sprintf("Q1_filtered:%d,Q2_filtered:%d,Q3_filtered:%d,Q4_filtered:%d",
		cfg.FilterAmount, cfg.ReducerQ2, cfg.FilterRangeQ3, cfg.FilterAndSplitterQ4)

	for i := range cfg.FilterCurrency {
		fmt.Fprintf(b, "  q1234_filter_currency_%d:\n", i)
		b.WriteString("    build:\n")
		b.WriteString("      context: ./src/\n")
		b.WriteString("      dockerfile: cmd/currencyfilter/Dockerfile\n")
		fmt.Fprintf(b, "    container_name: q1234_filter_currency_%d\n", i)
		rabbitmqDepends(b)
		b.WriteString("    environment:\n")
		fmt.Fprintf(b, "      - ID=%d\n", i)
		b.WriteString("      - MOM_PORT=5672\n")
		b.WriteString("      - MOM_HOST=rabbitmq\n")
		b.WriteString("      - INPUT_MIDDLEWARE_PREFIX=Q1234_filter_currency\n")
		fmt.Fprintf(b, "      - OUTPUT_CLUSTERS=%s\n", outputClusters)
		b.WriteString("      - CURRENCIES=US Dollar\n")
		b.WriteString("      - EXPECTED_EOFS=1\n")
		b.WriteString("      - QUERY_ID=1\n")
		fmt.Fprintf(b, "      - PERSIST_PATH=/var/bkp/q1234_filter_currency_%d\n", i)
		b.WriteString("      - PERSIST_BATCH_SIZE=50\n")
		b.WriteString("      - PERSIST_FLUSH_INTERVAL=1s\n")
		jsonFileLogging(b)
		b.WriteString("\n")
	}
}

func writeFilterAmount(b *strings.Builder, cfg *Config) {
	for i := range cfg.FilterAmount {
		fmt.Fprintf(b, "  q1_filter_amount_%d:\n", i)
		b.WriteString("    build:\n")
		b.WriteString("      context: ./src/\n")
		b.WriteString("      dockerfile: cmd/amountfilter/Dockerfile\n")
		fmt.Fprintf(b, "    container_name: q1_filter_amount_%d\n", i)
		rabbitmqDepends(b)
		b.WriteString("    environment:\n")
		fmt.Fprintf(b, "      - ID=%d\n", i)
		b.WriteString("      - MOM_PORT=5672\n")
		b.WriteString("      - MOM_HOST=rabbitmq\n")
		b.WriteString("      - INPUT_MIDDLEWARE_PREFIX=Q1_filtered\n")
		b.WriteString("      - OUTPUT_QUEUE=results_queue\n")
		b.WriteString("      - AMOUNT=50.0\n")
		fmt.Fprintf(b, "      - EXPECTED_EOFS=%d\n", cfg.FilterCurrency)
		b.WriteString("      - QUERY_ID=1\n")
		fmt.Fprintf(b, "      - PERSIST_PATH=/var/bkp/q1_filter_amount_%d\n", i)
		b.WriteString("      - PERSIST_BATCH_SIZE=50\n")
		b.WriteString("      - PERSIST_FLUSH_INTERVAL=1s\n")
		jsonFileLogging(b)
		b.WriteString("\n")
	}
}

func writeReducerQ2(b *strings.Builder, cfg *Config) {
	for i := range cfg.ReducerQ2 {
		fmt.Fprintf(b, "  q2_reducer_%d:\n", i)
		b.WriteString("    build:\n")
		b.WriteString("      context: ./src/\n")
		b.WriteString("      dockerfile: cmd/reducer/Dockerfile\n")
		fmt.Fprintf(b, "    container_name: q2_reducer_%d\n", i)
		rabbitmqDepends(b)
		b.WriteString("    environment:\n")
		fmt.Fprintf(b, "      - ID=%d\n", i)
		b.WriteString("      - MOM_HOST=rabbitmq\n")
		b.WriteString("      - MOM_PORT=5672\n")
		b.WriteString("      - QUERY_ID=2\n")
		b.WriteString("      - REDUCER_TYPE=MAX_AMOUNT_FROM_BANK\n")
		b.WriteString("      - INPUT_MIDDLEWARE_PREFIX=Q2_filtered\n")
		fmt.Fprintf(b, "      - EXPECTED_EOFS=%d\n", cfg.FilterCurrency)
		b.WriteString("      - OUTPUT_MIDDLEWARE_PREFIX=Q2_reduced_transfers\n")
		fmt.Fprintf(b, "      - OUTPUT_AMOUNT=%d\n", cfg.ReducerQ2)
		fmt.Fprintf(b, "      - PERSIST_PATH=/var/bkp/q2_reducer_%d\n", i)
		b.WriteString("      - PERSIST_BATCH_SIZE=50\n")
		b.WriteString("      - PERSIST_FLUSH_INTERVAL=1s\n")
		jsonFileLogging(b)
		b.WriteString("\n")
	}
}

func writeFilterBankIdAlreadySeen(b *strings.Builder, cfg *Config) {
	for i := range cfg.FilterBankIdAlreadySeen {
		fmt.Fprintf(b, "  q2_filter_bank_distinct_%d:\n", i)
		b.WriteString("    build:\n")
		b.WriteString("      context: ./src/\n")
		b.WriteString("      dockerfile: cmd/bankdistinctfilter/Dockerfile\n")
		fmt.Fprintf(b, "    container_name: q2_filter_bank_distinct_%d\n", i)
		rabbitmqDepends(b)
		b.WriteString("    environment:\n")
		b.WriteString("      - MOM_HOST=rabbitmq\n")
		b.WriteString("      - MOM_PORT=5672\n")
		fmt.Fprintf(b, "      - ID=%d\n", i)
		b.WriteString("      - INPUT_MIDDLEWARE_PREFIX=Q2_accounts\n")
		b.WriteString("      - OUTPUT_MIDDLEWARE_PREFIX=Q2_join_accounts\n")
		fmt.Fprintf(b, "      - OUTPUT_AMOUNT=%d\n", cfg.JoinQ2)
		b.WriteString("      - QUERY_ID=2\n")
		fmt.Fprintf(b, "      - PERSIST_PATH=/var/bkp/q2_filter_bank_distinct_%d\n", i)
		b.WriteString("      - PERSIST_BATCH_SIZE=50\n")
		b.WriteString("      - PERSIST_FLUSH_INTERVAL=1s\n")
		jsonFileLogging(b)
		b.WriteString("\n")
	}
}

func writeJoinQ2(b *strings.Builder, cfg *Config) {
	for i := range cfg.JoinQ2 {
		fmt.Fprintf(b, "  q2_join_%d:\n", i)
		b.WriteString("    build:\n")
		b.WriteString("      context: ./src/\n")
		b.WriteString("      dockerfile: cmd/join/Dockerfile\n")
		fmt.Fprintf(b, "    container_name: q2_join_%d\n", i)
		rabbitmqDepends(b)
		b.WriteString("    environment:\n")
		b.WriteString("      - MOM_HOST=rabbitmq\n")
		b.WriteString("      - MOM_PORT=5672\n")
		b.WriteString("      - JOIN_TYPE=transfer_account_by_bank\n")
		b.WriteString("      - LEFT_INPUT_MIDDLEWARE_PREFIX=Q2_reduced_transfers\n")
		b.WriteString("      - RIGHT_INPUT_MIDDLEWARE_PREFIX=Q2_join_accounts\n")
		b.WriteString("      - OUTPUT_QUEUE=results_queue\n")
		fmt.Fprintf(b, "      - LEFT_EOFS_EXPECTED=%d\n", cfg.ReducerQ2)
		fmt.Fprintf(b, "      - RIGHT_EOFS_EXPECTED=%d\n", cfg.FilterBankIdAlreadySeen)
		fmt.Fprintf(b, "      - ID=%d\n", i)
		fmt.Fprintf(b, "      - PERSIST_PATH=/var/bkp/q2_join_%d\n", i)
		b.WriteString("      - PERSIST_BATCH_SIZE=50\n")
		b.WriteString("      - PERSIST_FLUSH_INTERVAL=1s\n")
		jsonFileLogging(b)
		b.WriteString("\n")
	}
}

func writeFilterAndSplitterQ4(b *strings.Builder, cfg *Config) {
	for i := range cfg.FilterAndSplitterQ4 {
		fmt.Fprintf(b, "  q4_filter_and_splitter_%d:\n", i)
		b.WriteString("    build:\n")
		b.WriteString("      context: ./src/\n")
		b.WriteString("      dockerfile: cmd/filterandsplitter/Dockerfile\n")
		fmt.Fprintf(b, "    container_name: q4_filter_and_splitter_%d\n", i)
		rabbitmqDepends(b)
		b.WriteString("    environment:\n")
		fmt.Fprintf(b, "      - ID=%d\n", i)
		b.WriteString("      - MOM_HOST=rabbitmq\n")
		b.WriteString("      - MOM_PORT=5672\n")
		fmt.Fprintf(b, "      - EXPECTED_EOFS=%d\n", cfg.FilterCurrency)
		fmt.Fprintf(b, "      - OUTPUT_AMOUNT=%d\n", cfg.JoinAccountsQ4)
		b.WriteString("      - OUTPUT_MIDDLEWARE_PREFIX=Q4_FilterSplitter\n")
		b.WriteString("      - INPUT_MIDDLEWARE_PREFIX=Q4_filtered\n")
		b.WriteString("      - DATE_RANGE=2022-09-01 00:00:00,2022-09-06 00:00:00\n")
		b.WriteString("      - QUERY_ID=4\n")
		fmt.Fprintf(b, "      - PERSIST_PATH=/var/bkp/q4_filter_and_splitter_%d.bin\n", i)
		b.WriteString("      - PERSIST_BATCH_SIZE=100\n")
		b.WriteString("      - PERSIST_FLUSH_INTERVAL=1s\n")
		jsonFileLogging(b)
		b.WriteString("\n")
	}
}

func writeJoinAccountsQ4(b *strings.Builder, cfg *Config) {
	for i := range cfg.JoinAccountsQ4 {
		fmt.Fprintf(b, "  q4_join_accounts_%d:\n", i)
		b.WriteString("    build:\n")
		b.WriteString("      context: ./src/\n")
		b.WriteString("      dockerfile: cmd/joinaccounts/Dockerfile\n")
		fmt.Fprintf(b, "    container_name: q4_join_accounts_%d\n", i)
		rabbitmqDepends(b)
		b.WriteString("    environment:\n")
		fmt.Fprintf(b, "      - ID=%d\n", i)
		b.WriteString("      - MOM_HOST=rabbitmq\n")
		b.WriteString("      - MOM_PORT=5672\n")
		fmt.Fprintf(b, "      - OUTPUT_AMOUNT=%d\n", cfg.AcumAccountsQ4)
		b.WriteString("      - OUTPUT_MIDDLEWARE_PREFIX=Q4_JoinAccounts\n")
		b.WriteString("      - INPUT_MIDDLEWARE_PREFIX=Q4_FilterSplitter\n")
		fmt.Fprintf(b, "      - EXPECTED_EOFS=%d\n", cfg.FilterAndSplitterQ4)
		b.WriteString("      - QUALIFIED_EXCHANGE=Q4_qualified_accounts\n")
		fmt.Fprintf(b, "      - PEER_AMOUNT=%d\n", cfg.JoinAccountsQ4)
		b.WriteString("      - THRESHOLD=5\n")
		b.WriteString("      - QUERY_ID=4\n")
		writeMaxBatchConfig(b, cfg)
		fmt.Fprintf(b, "      - PERSIST_PATH=/var/bkp/q4_join_accounts_%d\n", i)
		b.WriteString("      - PERSIST_BATCH_SIZE=100\n")
		b.WriteString("      - PERSIST_FLUSH_INTERVAL=1s\n")
		jsonFileLogging(b)
		b.WriteString("\n")
	}
}

func writeAcumAccountsQ4(b *strings.Builder, cfg *Config) {
	for i := range cfg.AcumAccountsQ4 {
		fmt.Fprintf(b, "  q4_acum_accounts_%d:\n", i)
		b.WriteString("    build:\n")
		b.WriteString("      context: ./src/\n")
		b.WriteString("      dockerfile: cmd/acumaccounts/Dockerfile\n")
		fmt.Fprintf(b, "    container_name: q4_acum_accounts_%d\n", i)
		rabbitmqDepends(b)
		b.WriteString("    environment:\n")
		fmt.Fprintf(b, "      - ID=%d\n", i)
		b.WriteString("      - MOM_HOST=rabbitmq\n")
		b.WriteString("      - MOM_PORT=5672\n")
		fmt.Fprintf(b, "      - OUTPUT_AMOUNT=%d\n", cfg.FilterAccountSeenQ4)
		b.WriteString("      - OUTPUT_MIDDLEWARE_PREFIX=Q4_AcumAccounts\n")
		b.WriteString("      - INPUT_MIDDLEWARE_PREFIX=Q4_JoinAccounts\n")
		fmt.Fprintf(b, "      - EXPECTED_EOFS=%d\n", cfg.JoinAccountsQ4)
		b.WriteString("      - REQUIRED_AMT=5\n")
		b.WriteString("      - QUERY_ID=4\n")
		fmt.Fprintf(b, "      - PERSIST_PATH=/var/bkp/q4_acum_accounts_%d\n", i)
		b.WriteString("      - PERSIST_BATCH_SIZE=100\n")
		b.WriteString("      - PERSIST_FLUSH_INTERVAL=1s\n")
		jsonFileLogging(b)
		b.WriteString("\n")
	}
}

func writeFilterAccountSeenQ4(b *strings.Builder, cfg *Config) {
	for i := range cfg.FilterAccountSeenQ4 {
		fmt.Fprintf(b, "  q4_filter_account_seen_%d:\n", i)
		b.WriteString("    build:\n")
		b.WriteString("      context: ./src/\n")
		b.WriteString("      dockerfile: cmd/filteraccountseen/Dockerfile\n")
		fmt.Fprintf(b, "    container_name: q4_filter_account_seen_%d\n", i)
		rabbitmqDepends(b)
		b.WriteString("    environment:\n")
		fmt.Fprintf(b, "      - ID=%d\n", i)
		b.WriteString("      - MOM_HOST=rabbitmq\n")
		b.WriteString("      - MOM_PORT=5672\n")
		b.WriteString("      - INPUT_MIDDLEWARE_PREFIX=Q4_AcumAccounts\n")
		b.WriteString("      - OUTPUT_QUEUE=results_queue\n")
		fmt.Fprintf(b, "      - EXPECTED_EOFS=%d\n", cfg.AcumAccountsQ4)
		b.WriteString("      - QUERY_ID=4\n")
		writeMaxBatchConfig(b, cfg)
		fmt.Fprintf(b, "      - PERSIST_PATH=/var/bkp/q4_filter_account_seen_%d\n", i)
		b.WriteString("      - PERSIST_BATCH_SIZE=100\n")
		b.WriteString("      - PERSIST_FLUSH_INTERVAL=1s\n")
		jsonFileLogging(b)
		b.WriteString("\n")
	}
}

func writeFilterRangeQ3(b *strings.Builder, cfg *Config) {
	for i := range cfg.FilterRangeQ3 {
		fmt.Fprintf(b, "  q3_filter_range_%d:\n", i)
		b.WriteString("    build:\n")
		b.WriteString("      context: ./src/\n")
		b.WriteString("      dockerfile: cmd/daterangesplitter/Dockerfile\n")
		fmt.Fprintf(b, "    container_name: q3_filter_range_%d\n", i)
		rabbitmqDepends(b)
		b.WriteString("    environment:\n")
		fmt.Fprintf(b, "      - ID=%d\n", i)
		b.WriteString("      - MOM_HOST=rabbitmq\n")
		b.WriteString("      - MOM_PORT=5672\n")
		b.WriteString("      - INPUT_MIDDLEWARE_PREFIX=Q3_filtered\n")
		b.WriteString("      - AVG_OUTPUT_MIDDLEWARE_PREFIX=Q3_avg_exchange\n")
		fmt.Fprintf(b, "      - AVG_OUTPUT_AMOUNT=%d\n", cfg.SumQ3)
		b.WriteString("      - FILTER_OUTPUT_MIDDLEWARE_PREFIX=Q3_filter_exchange\n")
		fmt.Fprintf(b, "      - FILTER_OUTPUT_AMOUNT=%d\n", cfg.AverageFilterQ3)
		fmt.Fprintf(b, "      - EXPECTED_EOFS=%d\n", cfg.FilterCurrency)
		b.WriteString("      - QUERY_ID=3\n")
		fmt.Fprintf(b, "      - PERSIST_PATH=/var/bkp/q3_filter_range_%d\n", i)
		b.WriteString("      - PERSIST_BATCH_SIZE=50\n")
		b.WriteString("      - PERSIST_FLUSH_INTERVAL=1s\n")
		jsonFileLogging(b)
		b.WriteString("\n")
	}
}

func writeSumQ3(b *strings.Builder, cfg *Config) {
	for i := range cfg.SumQ3 {
		fmt.Fprintf(b, "  q3_sum_%d:\n", i)
		b.WriteString("    build:\n")
		b.WriteString("      context: ./src/\n")
		b.WriteString("      dockerfile: cmd/sum/Dockerfile\n")
		fmt.Fprintf(b, "    container_name: q3_sum_%d\n", i)
		rabbitmqDepends(b)
		b.WriteString("    environment:\n")
		fmt.Fprintf(b, "      - ID=%d\n", i)
		b.WriteString("      - MOM_HOST=rabbitmq\n")
		b.WriteString("      - MOM_PORT=5672\n")
		b.WriteString("      - INPUT_MIDDLEWARE_PREFIX=Q3_avg_exchange\n")
		b.WriteString("      - OUTPUT_MIDDLEWARE_PREFIX=Q3_sum_aggregate\n")
		fmt.Fprintf(b, "      - OUTPUT_AMOUNT=%d\n", cfg.AggregateQ3)
		fmt.Fprintf(b, "      - EXPECTED_EOFS=%d\n", cfg.FilterRangeQ3)
		writeMaxBatchConfig(b, cfg)
		b.WriteString("      - QUERY_ID=3\n")
		fmt.Fprintf(b, "      - PERSIST_PATH=/var/bkp/q3_sum_%d\n", i)
		b.WriteString("      - PERSIST_BATCH_SIZE=50\n")
		b.WriteString("      - PERSIST_FLUSH_INTERVAL=1s\n")
		jsonFileLogging(b)
		b.WriteString("\n")
	}
}

func writeAggregateQ3(b *strings.Builder, cfg *Config) {
	for i := range cfg.AggregateQ3 {
		fmt.Fprintf(b, "  q3_aggregate_%d:\n", i)
		b.WriteString("    build:\n")
		b.WriteString("      context: ./src/\n")
		b.WriteString("      dockerfile: cmd/aggregate/Dockerfile\n")
		fmt.Fprintf(b, "    container_name: q3_aggregate_%d\n", i)
		rabbitmqDepends(b)
		b.WriteString("    environment:\n")
		fmt.Fprintf(b, "      - ID=%d\n", i)
		b.WriteString("      - MOM_HOST=rabbitmq\n")
		b.WriteString("      - MOM_PORT=5672\n")
		b.WriteString("      - INPUT_MIDDLEWARE_PREFIX=Q3_sum_aggregate\n")
		b.WriteString("      - OUTPUT_MIDDLEWARE_PREFIX=Q3_avg_output\n")
		fmt.Fprintf(b, "      - EXPECTED_EOFS=%d\n", cfg.SumQ3)
		writeMaxBatchConfig(b, cfg)
		b.WriteString("      - QUERY_ID=3\n")
		fmt.Fprintf(b, "      - PERSIST_PATH=/var/bkp/q3_aggregate_%d\n", i)
		b.WriteString("      - PERSIST_BATCH_SIZE=50\n")
		b.WriteString("      - PERSIST_FLUSH_INTERVAL=1s\n")
		jsonFileLogging(b)
		b.WriteString("\n")
	}
}

func writeAverageFilterQ3(b *strings.Builder, cfg *Config) {
	for i := range cfg.AverageFilterQ3 {
		fmt.Fprintf(b, "  q3_average_filter_%d:\n", i)
		b.WriteString("    build:\n")
		b.WriteString("      context: ./src/\n")
		b.WriteString("      dockerfile: cmd/averagefilter/Dockerfile\n")
		fmt.Fprintf(b, "    container_name: q3_average_filter_%d\n", i)
		rabbitmqDepends(b)
		b.WriteString("    environment:\n")
		fmt.Fprintf(b, "      - ID=%d\n", i)
		b.WriteString("      - MOM_HOST=rabbitmq\n")
		b.WriteString("      - MOM_PORT=5672\n")
		b.WriteString("      - INPUT_MIDDLEWARE_PREFIX=Q3_filter_exchange\n")
		b.WriteString("      - AVG_INPUT_MIDDLEWARE_PREFIX=Q3_avg_output\n")
		fmt.Fprintf(b, "      - EXPECTED_EOFS=%d\n", cfg.FilterRangeQ3)
		fmt.Fprintf(b, "      - AVG_EXPECTED_EOFS=%d\n", cfg.AggregateQ3)
		b.WriteString("      - OUTPUT_QUEUE=results_queue\n")
		writeMaxBatchConfig(b, cfg)
		fmt.Fprintf(b, "      - PERSIST_PATH=/var/bkp/q3_average_filter_%d\n", i)
		b.WriteString("      - PERSIST_BATCH_SIZE=50\n")
		b.WriteString("      - PERSIST_FLUSH_INTERVAL=1s\n")
		b.WriteString("      - QUERY_ID=3\n")
		jsonFileLogging(b)
		b.WriteString("\n")
	}
}

func writeFilterDateAndPayment(b *strings.Builder, cfg *Config) {
	for i := range cfg.FilterDateAndPayment {
		fmt.Fprintf(b, "  q5_filter_date_and_payment_%d:\n", i)
		b.WriteString("    build:\n")
		b.WriteString("      context: ./src/\n")
		b.WriteString("      dockerfile: cmd/daterangeandpaymentfilter/Dockerfile\n")
		fmt.Fprintf(b, "    container_name: q5_filter_date_and_payment_%d\n", i)
		rabbitmqDepends(b)
		b.WriteString("    environment:\n")
		b.WriteString("      - MOM_HOST=rabbitmq\n")
		b.WriteString("      - MOM_PORT=5672\n")
		fmt.Fprintf(b, "      - ID=%d\n", i)
		b.WriteString("      - INPUT_MIDDLEWARE_PREFIX=Q5_filter\n")
		b.WriteString("      - OUTPUT_CLUSTERS=Q5_filtered:1\n")
		b.WriteString("      - EXPECTED_EOFS=1\n")
		b.WriteString("      - DATE_RANGE=2022-09-01 00:00:00,2022-09-06 00:00:00\n")
		b.WriteString("      - PAYMENT_FORMATS=ACH,Wire\n")
		b.WriteString("      - QUERY_ID=5\n")
		fmt.Fprintf(b, "      - PERSIST_PATH=/var/bkp/q5_filter_date_and_payment_%d\n", i)
		b.WriteString("      - PERSIST_BATCH_SIZE=50\n")
		b.WriteString("      - PERSIST_FLUSH_INTERVAL=1s\n")
		jsonFileLogging(b)
		b.WriteString("\n")
	}
}

func writeFilterAmtQ5(b *strings.Builder, cfg *Config) {
	for i := range cfg.FilterAmtQ5 {
		fmt.Fprintf(b, "  q5_filter_amount_%d:\n", i)
		b.WriteString("    build:\n")
		b.WriteString("      context: ./src/\n")
		b.WriteString("      dockerfile: cmd/convertedamountfilter/Dockerfile\n")
		fmt.Fprintf(b, "    container_name: q5_filter_amount_%d\n", i)
		rabbitmqDepends(b)
		b.WriteString("    environment:\n")
		b.WriteString("      - MOM_HOST=rabbitmq\n")
		b.WriteString("      - MOM_PORT=5672\n")
		fmt.Fprintf(b, "      - ID=%d\n", i)
		b.WriteString("      - AMOUNT=1\n")
		b.WriteString("      - INPUT_MIDDLEWARE_PREFIX=Q5_fetcher_output\n")
		b.WriteString("      - OUTPUT_QUEUE=Q5_filtered_to_count_q\n")
		b.WriteString("      - FILTER_AMOUNT=1\n")
		b.WriteString("      - QUERY_ID=5\n")
		b.WriteString("      - QUOTE=US Dollar\n")
		fmt.Fprintf(b, "      - PERSIST_PATH=/var/bkp/q5_filter_amount_%d\n", i)
		b.WriteString("      - PERSIST_BATCH_SIZE=50\n")
		b.WriteString("      - PERSIST_FLUSH_INTERVAL=1s\n")
		jsonFileLogging(b)
		b.WriteString("\n")
	}
}
