rm -rf res/
mkdir res

testTerm=4
cnt=0

for ((i=1; i<=$testTerm; i ++))
do
  res=`go test -run 3A`
  if echo $res | grep -q PASS;then
    echo "test_$i:PASS"
    cnt=$[$cnt+1]
  else
    echo "$res" > res/fail_$i.txt
    echo "test_$i:Failed"
  fi
done

if [ $cnt = $testTerm ];then
  echo "PASS"
else
  echo "Failed (Pass:$cnt/$testTerm)"
fi