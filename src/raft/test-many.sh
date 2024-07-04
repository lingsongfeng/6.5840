
for i in $(seq 1 1000); do
    echo TRIAL $i start
    go test -race -run 3C >> log.txt
    echo TRIAL $i done
done