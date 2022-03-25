#!/bin/bash

baseurl="https://api.coin.z.com/data/trades"

exchange_name=$1
trading_type=$2

symbol=$3
 
years=("2022")
months=(03)
dates=(24 25 26 27 28 29 30 31)

for year in ${years[@]}; do
  for month in ${months[@]}; do
    for date in ${dates[@]}; do
      url="${baseurl}/${symbol}/${year}/${month}/${year}${month}${date}_${symbol}.csv.gz"
      response=$(wget --server-response -q --spider ${url} 2>&1 | awk 'NR==1{print $2}')
      if [[ ${response} == '403' ]]; then
        echo "File not exist: ${url}"
      else
        parent_dir="./raw_data/${exchange_name}/${trading_type}/${symbol}"
        $(mkdir -p ${parent_dir})
        $(cd ${parent_dir} && curl -LO -s ${url})
        $(cd ${parent_dir} && gzip -d ${year}${month}${date}_${symbol}.csv.gz)
        echo "downloaded: ${parent_dir}/${symbol}${year}-${month}-${date}.csv"
      fi
    done
  done
done

