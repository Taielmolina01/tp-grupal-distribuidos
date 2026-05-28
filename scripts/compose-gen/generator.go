package main

import (
	"fmt"
	"strings"
)

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

	return b.String()
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
	accountQueues := queues("accounts", cfg.FilterBankIdAlreadySeen)
	queryEofs := fmt.Sprintf("1:1,2:%d,3:1,4:%d,5:1", cfg.JoinQ2, cfg.FilterAccountSeenQ4)

	b.WriteString("  gateway:\n")
	b.WriteString("    build:\n")
	b.WriteString("      context: ./src/\n")
	b.WriteString("      dockerfile: cmd/gateway/Dockerfile\n")
	b.WriteString("    container_name: gateway\n")
	rabbitmqDepends(b)
	b.WriteString("    environment:\n")
	fmt.Fprintf(b, "      - ACCOUNT_QUEUES=%s\n", accountQueues)
	b.WriteString("      - TRANSFERS_EXCHANGE=transfers_exchange\n")
	b.WriteString("      - TRANSFERS_ROUTING_KEYS=TRANSFERS_Q5_KEY,TRANSFERS_Q1234_KEY\n")
	b.WriteString("      - RESULTS_QUEUE=results_queue\n")
	b.WriteString("      - MOM_HOST=rabbitmq\n")
	b.WriteString("      - MOM_PORT=5672\n")
	b.WriteString("      - SERVER_HOST=gateway\n")
	b.WriteString("      - SERVER_PORT=5678\n")
	fmt.Fprintf(b, "      - QUERY_EOFS_EXPECTED=%s\n", queryEofs)
	b.WriteString("\n")
}

func writeFetcher(b *strings.Builder, cfg *Config) {
	outputQueues := queues("fetcher_output", cfg.FilterAmtQ5)

	b.WriteString("  fetcher:\n")
	b.WriteString("    build:\n")
	b.WriteString("      context: ./src/\n")
	b.WriteString("      dockerfile: cmd/fetcher/Dockerfile\n")
	b.WriteString("    container_name: fetcher\n")
	rabbitmqDepends(b)
	b.WriteString("    environment:\n")
	b.WriteString("      - MOM_HOST=rabbitmq\n")
	b.WriteString("      - MOM_PORT=5672\n")
	b.WriteString("      - INPUT_QUEUE=filtered_transfers_q5_fetcher\n")
	b.WriteString("      - INPUT_EXCHANGE=filtered_date_and_payment_exchange\n")
	b.WriteString("      - INPUT_ROUTING_KEYS=TRANSFERS_Q5_KEY\n")
	fmt.Fprintf(b, "      - OUTPUT_QUEUES=%s\n", outputQueues)
	b.WriteString("      - QUERY_ID=5\n")
	b.WriteString("      - QUOTE=USD\n")
	b.WriteString("\n")
}

func writeCountReducerQ5(b *strings.Builder, cfg *Config) {
	b.WriteString("  count_reducer_q5:\n")
	b.WriteString("    build:\n")
	b.WriteString("      context: ./src/\n")
	b.WriteString("      dockerfile: cmd/reducer/Dockerfile\n")
	b.WriteString("    container_name: count_reducer_q5\n")
	rabbitmqDepends(b)
	b.WriteString("    environment:\n")
	b.WriteString("      - MOM_HOST=rabbitmq\n")
	b.WriteString("      - MOM_PORT=5672\n")
	b.WriteString("      - ID=0\n")
	b.WriteString("      - REDUCER_AMOUNT=1\n")
	fmt.Fprintf(b, "      - INPUT_EOFS_EXPECTED=%d\n", cfg.FilterAmtQ5)
	b.WriteString("      - INPUT_QUEUE=filtered_to_count_q5\n")
	b.WriteString("      - OUTPUT_QUEUES=results_queue\n")
	b.WriteString("      - REDUCER_TYPE=COUNT\n")
	b.WriteString("      - QUERY_ID=5\n")
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
		b.WriteString("    volumes:\n")
		b.WriteString("      - ./input:/input\n")
		b.WriteString("      - ./output:/output\n")
		b.WriteString("\n")
	}
}

func writeFilterCurrency(b *strings.Builder, cfg *Config) {
	for i := range cfg.FilterCurrency {
		fmt.Fprintf(b, "  filter_currency_%d:\n", i)
		b.WriteString("    build:\n")
		b.WriteString("      context: ./src/\n")
		b.WriteString("      dockerfile: cmd/filter/Dockerfile\n")
		fmt.Fprintf(b, "    container_name: filter_currency_%d\n", i)
		rabbitmqDepends(b)
		b.WriteString("    environment:\n")
		fmt.Fprintf(b, "      - ID=%d\n", i)
		b.WriteString("      - MOM_PORT=5672\n")
		b.WriteString("      - MOM_HOST=rabbitmq\n")
		b.WriteString("      - INPUT_EXCHANGE=transfers_exchange\n")
		b.WriteString("      - INPUT_QUEUE=transfers_queue_q1234\n")
		b.WriteString("      - INPUT_ROUTING_KEYS=TRANSFERS_Q1234_KEY\n")
		b.WriteString("      - OUTPUT_EXCHANGE=filtered_transfers_exchange\n")
		b.WriteString("      - OUTPUT_QUEUE=filtered_transfers_q\n")
		b.WriteString("      - OUTPUT_ROUTING_KEYS=transfers_q1234\n")
		b.WriteString("      - CURRENCIES=US Dollar\n")
		b.WriteString("      - FILTER_TYPE=CURRENCY\n")
		fmt.Fprintf(b, "      - FILTER_AMOUNT=%d\n", cfg.FilterCurrency)
		b.WriteString("      - QUERY_ID=1\n")
		jsonFileLogging(b)
		b.WriteString("\n")
	}
}

func writeFilterAmount(b *strings.Builder, cfg *Config) {
	for i := range cfg.FilterAmount {
		fmt.Fprintf(b, "  filter_amount_%d:\n", i)
		b.WriteString("    build:\n")
		b.WriteString("      context: ./src/\n")
		b.WriteString("      dockerfile: cmd/filter/Dockerfile\n")
		fmt.Fprintf(b, "    container_name: filter_amount_%d\n", i)
		rabbitmqDepends(b)
		b.WriteString("    environment:\n")
		fmt.Fprintf(b, "      - ID=%d\n", i)
		b.WriteString("      - MOM_PORT=5672\n")
		b.WriteString("      - MOM_HOST=rabbitmq\n")
		b.WriteString("      - INPUT_EXCHANGE=filtered_transfers_exchange\n")
		b.WriteString("      - INPUT_QUEUE=filtered_transfers_q\n")
		b.WriteString("      - INPUT_ROUTING_KEYS=transfers_q1234\n")
		b.WriteString("      - OUTPUT_EXCHANGE=results_queue\n")
		b.WriteString("      - OUTPUT_QUEUE=results_queue\n")
		b.WriteString("      - OUTPUT_ROUTING_KEYS=results_queue\n")
		b.WriteString("      - AMOUNT=50.0\n")
		b.WriteString("      - FILTER_TYPE=AMOUNT\n")
		fmt.Fprintf(b, "      - FILTER_AMOUNT=%d\n", cfg.FilterAmount)
		b.WriteString("      - QUERY_ID=1\n")
		jsonFileLogging(b)
		b.WriteString("\n")
	}
}

func writeReducerQ2(b *strings.Builder, cfg *Config) {
	outputQueues := queues("reduced_transfers_q2", cfg.ReducerQ2)

	for i := range cfg.ReducerQ2 {
		fmt.Fprintf(b, "  reducer_q2_%d:\n", i)
		b.WriteString("    build:\n")
		b.WriteString("      context: ./src/\n")
		b.WriteString("      dockerfile: cmd/reducer/Dockerfile\n")
		fmt.Fprintf(b, "    container_name: reducer_q2_%d\n", i)
		rabbitmqDepends(b)
		b.WriteString("    environment:\n")
		fmt.Fprintf(b, "      - ID=%d\n", i)
		fmt.Fprintf(b, "      - REDUCER_AMOUNT=%d\n", cfg.ReducerQ2)
		b.WriteString("      - MOM_HOST=rabbitmq\n")
		b.WriteString("      - MOM_PORT=5672\n")
		b.WriteString("      - INPUT_EXCHANGE=filtered_transfers_exchange\n")
		b.WriteString("      - INPUT_QUEUE=reducer_q2_in_q\n")
		b.WriteString("      - INPUT_ROUTING_KEYS=transfers_q1234\n")
		fmt.Fprintf(b, "      - OUTPUT_QUEUES=%s\n", outputQueues)
		b.WriteString("      - OUTPUT_ROUTING_KEYS=transfers_q1234\n")
		b.WriteString("      - QUERY_ID=2\n")
		b.WriteString("      - REDUCER_TYPE=MAX_AMOUNT_FROM_BANK\n")
		jsonFileLogging(b)
		b.WriteString("\n")
	}
}

func writeFilterBankIdAlreadySeen(b *strings.Builder, cfg *Config) {
	outputQueues := queues("join_q2_accounts_q", cfg.JoinQ2)

	for i := range cfg.FilterBankIdAlreadySeen {
		fmt.Fprintf(b, "  filter_bank_id_already_seen_%d:\n", i)
		b.WriteString("    build:\n")
		b.WriteString("      context: ./src/\n")
		b.WriteString("      dockerfile: cmd/filter/Dockerfile\n")
		fmt.Fprintf(b, "    container_name: filter_bank_id_already_seen_%d\n", i)
		rabbitmqDepends(b)
		b.WriteString("    environment:\n")
		b.WriteString("      - MOM_HOST=rabbitmq\n")
		b.WriteString("      - MOM_PORT=5672\n")
		fmt.Fprintf(b, "      - ID=%d\n", i)
		fmt.Fprintf(b, "      - INPUT_QUEUE=accounts_%d\n", i)
		fmt.Fprintf(b, "      - OUTPUT_QUEUES=%s\n", outputQueues)
		fmt.Fprintf(b, "      - FILTER_AMOUNT=%d\n", cfg.FilterBankIdAlreadySeen)
		b.WriteString("      - FILTER_TYPE=BANK_DISTINCT\n")
		b.WriteString("      - QUERY_ID=2\n")
		b.WriteString("\n")
	}
}

func writeJoinQ2(b *strings.Builder, cfg *Config) {
	for i := range cfg.JoinQ2 {
		fmt.Fprintf(b, "  join_q2_%d:\n", i)
		b.WriteString("    build:\n")
		b.WriteString("      context: ./src/\n")
		b.WriteString("      dockerfile: cmd/join/Dockerfile\n")
		fmt.Fprintf(b, "    container_name: join_q2_%d\n", i)
		rabbitmqDepends(b)
		b.WriteString("    environment:\n")
		b.WriteString("      - MOM_HOST=rabbitmq\n")
		b.WriteString("      - MOM_PORT=5672\n")
		b.WriteString("      - JOIN_TYPE=transfer_account_by_bank\n")
		fmt.Fprintf(b, "      - LEFT_INPUT_QUEUE=reduced_transfers_q2_%d\n", i)
		fmt.Fprintf(b, "      - RIGHT_INPUT_QUEUE=join_q2_accounts_q_%d\n", i)
		b.WriteString("      - OUTPUT_EXCHANGE=results_queue\n")
		b.WriteString("      - OUTPUT_QUEUE=results_queue\n")
		b.WriteString("      - OUTPUT_ROUTING_KEYS=results_queue\n")
		fmt.Fprintf(b, "      - LEFT_EOFS_EXPECTED=%d\n", cfg.ReducerQ2)
		fmt.Fprintf(b, "      - RIGHT_EOFS_EXPECTED=%d\n", cfg.Clients)
		fmt.Fprintf(b, "      - JOIN_AMOUNT=%d\n", cfg.JoinQ2)
		fmt.Fprintf(b, "      - ID=%d\n", i)
		jsonFileLogging(b)
		b.WriteString("\n")
	}
}

func writeFilterAndSplitterQ4(b *strings.Builder, cfg *Config) {
	for i := range cfg.FilterAndSplitterQ4 {
		fmt.Fprintf(b, "  filter_and_splitter_q4_%d:\n", i)
		b.WriteString("    build:\n")
		b.WriteString("      context: ./src/\n")
		b.WriteString("      dockerfile: cmd/filterandsplitter/Dockerfile\n")
		fmt.Fprintf(b, "    container_name: filter_and_splitter_q4_%d\n", i)
		rabbitmqDepends(b)
		b.WriteString("    environment:\n")
		fmt.Fprintf(b, "      - ID=%d\n", i)
		b.WriteString("      - MOM_HOST=rabbitmq\n")
		b.WriteString("      - MOM_PORT=5672\n")
		fmt.Fprintf(b, "      - FILTER_AMOUNT=%d\n", cfg.FilterAndSplitterQ4)
		fmt.Fprintf(b, "      - OUTPUT_AMOUNT=%d\n", cfg.JoinAccountsQ4)
		b.WriteString("      - OUTPUT_MIDDLEWARE_PREFIX=filter_splitter_q4\n")
		b.WriteString("      - INPUT_EXCHANGE=filtered_transfers_exchange\n")
		b.WriteString("      - INPUT_QUEUE=filter_range_q4_in\n")
		b.WriteString("      - INPUT_ROUTING_KEYS=transfers_q1234\n")
		b.WriteString("      - DATE_RANGE=2022-09-01 00:00:00,2022-09-06 00:00:00\n")
		b.WriteString("      - QUERY_ID=4\n")
		jsonFileLogging(b)
		b.WriteString("\n")
	}
}

func writeJoinAccountsQ4(b *strings.Builder, cfg *Config) {
	for i := range cfg.JoinAccountsQ4 {
		fmt.Fprintf(b, "  join_accounts_q4_%d:\n", i)
		b.WriteString("    build:\n")
		b.WriteString("      context: ./src/\n")
		b.WriteString("      dockerfile: cmd/joinaccounts/Dockerfile\n")
		fmt.Fprintf(b, "    container_name: join_accounts_q4_%d\n", i)
		rabbitmqDepends(b)
		b.WriteString("    environment:\n")
		fmt.Fprintf(b, "      - ID=%d\n", i)
		b.WriteString("      - MOM_HOST=rabbitmq\n")
		b.WriteString("      - MOM_PORT=5672\n")
		fmt.Fprintf(b, "      - OUTPUT_AMOUNT=%d\n", cfg.AcumAccountsQ4)
		b.WriteString("      - OUTPUT_MIDDLEWARE_PREFIX=join_accounts_q4\n")
		b.WriteString("      - INPUT_MIDDLEWARE_PREFIX=filter_splitter_q4\n")
		b.WriteString("      - QUERY_ID=4\n")
		jsonFileLogging(b)
		b.WriteString("\n")
	}
}

func writeAcumAccountsQ4(b *strings.Builder, cfg *Config) {
	for i := range cfg.AcumAccountsQ4 {
		fmt.Fprintf(b, "  acum_accounts_q4_%d:\n", i)
		b.WriteString("    build:\n")
		b.WriteString("      context: ./src/\n")
		b.WriteString("      dockerfile: cmd/acumaccounts/Dockerfile\n")
		fmt.Fprintf(b, "    container_name: acum_accounts_q4_%d\n", i)
		rabbitmqDepends(b)
		b.WriteString("    environment:\n")
		fmt.Fprintf(b, "      - ID=%d\n", i)
		b.WriteString("      - MOM_HOST=rabbitmq\n")
		b.WriteString("      - MOM_PORT=5672\n")
		fmt.Fprintf(b, "      - OUTPUT_AMOUNT=%d\n", cfg.FilterAccountSeenQ4)
		b.WriteString("      - OUTPUT_MIDDLEWARE_PREFIX=acum_accounts_q4\n")
		b.WriteString("      - INPUT_MIDDLEWARE_PREFIX=join_accounts_q4\n")
		fmt.Fprintf(b, "      - EXPECTED_EOFS=%d\n", cfg.JoinAccountsQ4)
		b.WriteString("      - REQUIRED_AMT=5\n")
		b.WriteString("      - QUERY_ID=4\n")
		jsonFileLogging(b)
		b.WriteString("\n")
	}
}

func writeFilterAccountSeenQ4(b *strings.Builder, cfg *Config) {
	for i := range cfg.FilterAccountSeenQ4 {
		fmt.Fprintf(b, "  filter_account_seen_q4_%d:\n", i)
		b.WriteString("    build:\n")
		b.WriteString("      context: ./src/\n")
		b.WriteString("      dockerfile: cmd/filteraccountseen/Dockerfile\n")
		fmt.Fprintf(b, "    container_name: filter_account_seen_q4_%d\n", i)
		rabbitmqDepends(b)
		b.WriteString("    environment:\n")
		fmt.Fprintf(b, "      - ID=%d\n", i)
		b.WriteString("      - MOM_HOST=rabbitmq\n")
		b.WriteString("      - MOM_PORT=5672\n")
		b.WriteString("      - INPUT_MIDDLEWARE_PREFIX=acum_accounts_q4\n")
		b.WriteString("      - OUTPUT_MIDDLEWARE=results_queue\n")
		fmt.Fprintf(b, "      - EXPECTED_EOFS=%d\n", cfg.AcumAccountsQ4)
		b.WriteString("      - QUERY_ID=4\n")
		jsonFileLogging(b)
		b.WriteString("\n")
	}
}

func writeFilterRangeQ3(b *strings.Builder, cfg *Config) {
	for i := range cfg.FilterRangeQ3 {
		fmt.Fprintf(b, "  filter_range_q3_%d:\n", i)
		b.WriteString("    build:\n")
		b.WriteString("      context: ./src/\n")
		b.WriteString("      dockerfile: cmd/filter/Dockerfile\n")
		fmt.Fprintf(b, "    container_name: filter_range_q3_%d\n", i)
		rabbitmqDepends(b)
		b.WriteString("    environment:\n")
		fmt.Fprintf(b, "      - ID=%d\n", i)
		b.WriteString("      - MOM_HOST=rabbitmq\n")
		b.WriteString("      - MOM_PORT=5672\n")
		b.WriteString("      - INPUT_EXCHANGE=filtered_transfers_exchange\n")
		b.WriteString("      - INPUT_QUEUE=filter_range_q3_in\n")
		b.WriteString("      - INPUT_ROUTING_KEYS=transfers_q1234\n")
		b.WriteString("      - OUTPUT_QUEUES=transfers_avg_period_q,transfers_filter_period_q\n")
		fmt.Fprintf(b, "      - FILTER_AMOUNT=%d\n", cfg.FilterRangeQ3)
		b.WriteString("      - FILTER_TYPE=DATE_RANGE_AND_SPLITTER\n")
		b.WriteString("      - QUERY_ID=3\n")
		b.WriteString("\n")
	}
}

func writeSumQ3(b *strings.Builder, cfg *Config) {
	outputQueues := queues("sum_aggregate_q3", cfg.AggregateQ3)

	for i := range cfg.SumQ3 {
		fmt.Fprintf(b, "  sum_q3_%d:\n", i)
		b.WriteString("    build:\n")
		b.WriteString("      context: ./src/\n")
		b.WriteString("      dockerfile: cmd/sum/Dockerfile\n")
		fmt.Fprintf(b, "    container_name: sum_q3_%d\n", i)
		rabbitmqDepends(b)
		b.WriteString("    environment:\n")
		fmt.Fprintf(b, "      - ID=%d\n", i)
		fmt.Fprintf(b, "      - SUM_AMOUNT=%d\n", cfg.SumQ3)
		b.WriteString("      - MOM_HOST=rabbitmq\n")
		b.WriteString("      - MOM_PORT=5672\n")
		b.WriteString("      - INPUT_QUEUE=transfers_avg_period_q\n")
		fmt.Fprintf(b, "      - OUTPUT_QUEUES=%s\n", outputQueues)
		b.WriteString("      - QUERY_ID=3\n")
		b.WriteString("\n")
	}
}

func writeAggregateQ3(b *strings.Builder, cfg *Config) {
	outputQueues := queues("filter2_avg_q3", cfg.AverageFilterQ3)

	for i := range cfg.AggregateQ3 {
		fmt.Fprintf(b, "  aggregate_q3_%d:\n", i)
		b.WriteString("    build:\n")
		b.WriteString("      context: ./src/\n")
		b.WriteString("      dockerfile: cmd/aggregate/Dockerfile\n")
		fmt.Fprintf(b, "    container_name: aggregate_q3_%d\n", i)
		rabbitmqDepends(b)
		b.WriteString("    environment:\n")
		fmt.Fprintf(b, "      - ID=%d\n", i)
		fmt.Fprintf(b, "      - AGGREGATE_AMOUNT=%d\n", cfg.AggregateQ3)
		b.WriteString("      - MOM_HOST=rabbitmq\n")
		b.WriteString("      - MOM_PORT=5672\n")
		fmt.Fprintf(b, "      - INPUT_QUEUE=sum_aggregate_q3_%d\n", i)
		fmt.Fprintf(b, "      - OUTPUT_QUEUES=%s\n", outputQueues)
		b.WriteString("      - QUERY_ID=3\n")
		b.WriteString("\n")
	}
}

func writeAverageFilterQ3(b *strings.Builder, cfg *Config) {
	for i := range cfg.AverageFilterQ3 {
		fmt.Fprintf(b, "  average_filter_q3_%d:\n", i)
		b.WriteString("    build:\n")
		b.WriteString("      context: ./src/\n")
		b.WriteString("      dockerfile: cmd/filter/Dockerfile\n")
		fmt.Fprintf(b, "    container_name: average_filter_q3_%d\n", i)
		rabbitmqDepends(b)
		b.WriteString("    environment:\n")
		fmt.Fprintf(b, "      - ID=%d\n", i)
		b.WriteString("      - MOM_HOST=rabbitmq\n")
		b.WriteString("      - MOM_PORT=5672\n")
		b.WriteString("      - FILTER_TYPE=AVERAGE_FILTER\n")
		fmt.Fprintf(b, "      - FILTER_AMOUNT=%d\n", cfg.AverageFilterQ3)
		b.WriteString("      - INPUT_QUEUE=transfers_filter_period_q\n")
		fmt.Fprintf(b, "      - AVG_INPUT_QUEUE=filter2_avg_q3_%d\n", i)
		fmt.Fprintf(b, "      - AVG_EXPECTED_EOFS=%d\n", cfg.AggregateQ3)
		b.WriteString("      - OUTPUT_QUEUE=results_queue\n")
		b.WriteString("      - QUERY_ID=3\n")
		b.WriteString("\n")
	}
}

func writeFilterDateAndPayment(b *strings.Builder, cfg *Config) {
	for i := range cfg.FilterDateAndPayment {
		fmt.Fprintf(b, "  filter_date_and_payment_%d:\n", i)
		b.WriteString("    build:\n")
		b.WriteString("      context: ./src/\n")
		b.WriteString("      dockerfile: cmd/filter/Dockerfile\n")
		fmt.Fprintf(b, "    container_name: filter_date_and_payment_%d\n", i)
		rabbitmqDepends(b)
		b.WriteString("    environment:\n")
		b.WriteString("      - MOM_HOST=rabbitmq\n")
		b.WriteString("      - MOM_PORT=5672\n")
		fmt.Fprintf(b, "      - ID=%d\n", i)
		b.WriteString("      - INPUT_EXCHANGE=transfers_exchange\n")
		b.WriteString("      - INPUT_ROUTING_KEYS=TRANSFERS_Q5_KEY\n")
		b.WriteString("      - INPUT_QUEUE=transfers_queue_q5\n")
		b.WriteString("      - OUTPUT_EXCHANGE=filtered_date_and_payment_exchange\n")
		b.WriteString("      - OUTPUT_ROUTING_KEYS=TRANSFERS_Q5_KEY\n")
		fmt.Fprintf(b, "      - FILTER_AMOUNT=%d\n", cfg.FilterDateAndPayment)
		b.WriteString("      - FILTER_TYPE=DATE_RANGE_AND_PAYMENT\n")
		b.WriteString("      - DATE_RANGE=2022-09-01 00:00:00,2022-09-06 00:00:00\n")
		b.WriteString("      - PAYMENT_FORMATS=ACH,Wire\n")
		b.WriteString("      - QUERY_ID=5\n")
		b.WriteString("\n")
	}
}

func writeFilterAmtQ5(b *strings.Builder, cfg *Config) {
	for i := range cfg.FilterAmtQ5 {
		fmt.Fprintf(b, "  filter_amt_q5_%d:\n", i)
		b.WriteString("    build:\n")
		b.WriteString("      context: ./src/\n")
		b.WriteString("      dockerfile: cmd/filter/Dockerfile\n")
		fmt.Fprintf(b, "    container_name: filter_amt_q5_%d\n", i)
		rabbitmqDepends(b)
		b.WriteString("    environment:\n")
		b.WriteString("      - MOM_HOST=rabbitmq\n")
		b.WriteString("      - MOM_PORT=5672\n")
		fmt.Fprintf(b, "      - ID=%d\n", i)
		b.WriteString("      - AMOUNT=1\n")
		fmt.Fprintf(b, "      - LEFT_INPUT_QUEUE=fetcher_output_%d\n", i)
		b.WriteString("      - RIGHT_INPUT_QUEUE=filtered_transfers_q5_filter\n")
		b.WriteString("      - RIGHT_INPUT_EXCHANGE=filtered_date_and_payment_exchange\n")
		b.WriteString("      - RIGHT_INPUT_ROUTING_KEYS=TRANSFERS_Q5_KEY\n")
		b.WriteString("      - OUTPUT_QUEUE=filtered_to_count_q5\n")
		fmt.Fprintf(b, "      - FILTER_AMOUNT=%d\n", cfg.FilterAmtQ5)
		b.WriteString("      - FILTER_TYPE=CONVERTED_AMOUNT_FILTER\n")
		b.WriteString("      - QUERY_ID=5\n")
		b.WriteString("\n")
	}
}
