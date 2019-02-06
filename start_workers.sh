root_dir='/home/kuznetsov/models_env/tw/ready/arch'
worker=$PWD/bash_worker.py
cpu_n=35
version=1.0
echo ${root_dir}
echo ${worker}
echo ${cpu_n}
echo ${version}
for year in ${root_dir}/201[0-9]
do 
echo $year
cd $year
    for month in ./[0-9][0-9]
    do 
    echo $month
    cd $month
    python $worker -d './*/*/*' -t ${year}-${month:2:10}-v${version}-df.pkl -n ${cpu_n}
    cd ..
    done
done
