master:
	iex --sname master -S mix

worker:
	MR_MODE=worker MR_NAME=${NAME} iex --sname ${NAME} -S mix
