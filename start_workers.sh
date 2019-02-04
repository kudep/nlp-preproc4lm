root_dir = '/home/kuznetsov/models_env/tw/ready/arch'
worker = $PWD/bash_worker.py
cpu_n = 40
for year in ${root_dir}/*
do 
cd $year
    for month in ./*
    do 
    echo $month
    cd $month
    python $worker -d './*/*/*' -t ../${month}.csv -n ${cpu_n}
    cd ..
    done
done
