#coordinator
#go run mrcoordinator.go  pg*.txt

#worker
#zsh ./deleteMR.sh 
#go build -buildmode=plugin ../mrapps/wc.go
#go run mrworker.go wc.so

rm mr-*