#!/bin/bash
set +x
function hadoop_del() {
    [[ $# < 1 ]] && return -1
    local path="$1"
    ${AFS_CLIENT} fs -Dhadoop.job.ugi=ark,ark_saas -rmr $path
}

function hadoop_make_done() {
    [[ $# < 1 ]] && return -1
    local path="$1"
    ${AFS_CLIENT} fs -Dhadoop.job.ugi=ark,ark_saas -touchz ${path}/to.hadoop.done
}

function email() {
    echo $2 | mail -s "$1" duxiaoyan@baidu.com 
}

die() {
    echo $1
    exit 1
}

function wait_done_file() {
    [ $# -ne 2 ] && {
        die "usage: wait_done_file timeout donefile"
    }

    timeout=$1
    donefile=$2

    check_interval=300
    check_times=`expr $timeout / $check_interval`
    ready=0
    for i in `seq 0 $check_times`
    do
        ${TURING_CLIENT} fs -test -e ${donefile}
        if [ $? -eq 0 ];then
            ready=1
            break
        fi
        sleep $check_interval
    done

    if [ $ready -eq 1 ];then
        echo "$donefile is ready!"
        return 0
    else
        email "wait_done_file failed" "$donefile does not exist!"
        return 1
    fi
}


function get_time() {
    case "$1" in
        "-tf" ) # time formated
            date +"%Y-%m-%d %H:%M:%S"
            ;;
        "-df" ) # date formated
            date +"%Y-%m-%d"
            ;;
        "-d" )
            date +"%Y%m%d"
            ;;
        "-t" )
            date +%"Y%m%d%H%M%S"
            ;;
        * )
            date +%"Y%m%d%H%M%S"
            ;;
    esac
}

# para end_date data_len
function gen_date_range_end_len {
    [[ $# < 2 ]] && return -1
    local end_date=`date --date="$1" +%Y%m%d`
    local date_len=$2
    local dem=${3:-","}
    local DATE_RANGE=""

    while [[ $date_len > 0 ]]
    do
        DATE_RANGE="${DATE_RANGE}${dem}$end_date"
        end_date=`date --date="$end_date 1 day ago" +%Y%m%d`
        let "date_len--"
    done
    DATE_RANGE="${DATE_RANGE:1}"
    echo $DATE_RANGE
}


# para start_date end_date
function gen_date_range_start_end {
    [[ $# < 2 ]] && return -1
    local start_date=$1
    local end_date=$2
    local dem=${3:-","}
    local sd=`date -d "$start_date" +%Y-%m-%d`
    local ed=`date -d "$end_date" +%Y-%m-%d`

    local DATE_RANGE=""
    while [[ $sd < $ed || $sd == $ed ]]
    do
        local DD=`date -d "$ed" +%Y%m%d`
        DATE_RANGE="${DATE_RANGE}${dem}$DD"
        ed=`date -d "$ed 1 day ago" +%Y-%m-%d`
    done
    DATE_RANGE="${DATE_RANGE:1}"
    echo $DATE_RANGE
}


#  $1  out_path : done.txt中第一列 集群目录
#  $2  put_path : done.txt存放目录，默认为 out_path上一级
#  $3  time     : done.txt中第3列 生成时间，默认为当前时间
function make_done_txt {
    cur_time=`date +%Y%m%d%H%M%S`
	[[ $# < 1 ]] && return -1
	local out_path="$1"
	local put_path="${2:-${out_path%/*}}"
	local ts=${3:-$cur_time}
	echo -e "${out_path}\t${ts}" > ./tmpdone.txt
	${AFS_CLIENT} fs -Dhadoop.job.ugi=ark,ark_saas -rm "${put_path}/done.txt.bak"
	${AFS_CLIENT} fs -Dhadoop.job.ugi=ark,ark_saas -mv "${put_path}/done.txt" "${put_path}/done.txt.bak"
	${AFS_CLIENT} fs -Dhadoop.job.ugi=ark,ark_saas -put ./tmpdone.txt "${put_path}/done.txt"
}


# input: hadoop_path [--human/-h]
function hadoop_dus() {
	local human_flag=
	for _path in "$@"
	do
		if [[ "${_path}" == "--human" || "${_path}" == "-h" ]]; then
			human_flag=1
		fi
	done
	for _path in "$@"
	do
		[[ "${_path}" == "--human" || "${_path}" == "-h" ]] && continue
		if [[ -n "$human_flag" ]]; then
			${ASF_CLIENT} fs -Dhadoop.job.ugi=ark,ark_saas -dus "${_path}" | tr -s ' ' | awk -F' ' 'function human(x) { s="BKMGT"; while (x>=1024 && length(s)>1) {x/=1024; s=substr(s,2)}; s=substr(s,1,1); xf=(s=="B")?"%d":"%.2f" ; return sprintf(xf"%s", x, s) } {gsub($2, human($2)); print}'  | tr ' ' '\t'
		else
			${AFS_CLIENT} fs -Dhadoop.job.ugi=ark,ark_saas -dus "${_path}"  | tr -s ' ' | tr ' ' '\t'
		fi
	done
}


# input: hadoop_path
function hadoop_getsize() {
	local hadoop_path=${1?no hadoop path}
	hadoop_dus "$hadoop_path" | cut -f2
}


# input: hadoop_path [great_than_size],  default 0
function hadoop_checksize() {
	local hadoop_path=${1?no hadoop path}
	local size=${2:-0}
	local dir_size=$(hadoop_getsize "$hadoop_path")
	if [[ "$dir_size" -gt "$size" ]]; then
		return 0
	else
		return 1
	fi
}

# $1: hadoop任务返回值
# $2: hadoop输出目录
function check_hadoop_task_result() {
	local hadoop_task_ret=${1? no hadoop task return status}
	local hadoop_task_output=${2? no hadoop output}
	local hadoop_task_output_size=${3:0}
	if [[ "$hadoop_task_ret" != 0 ]]; then
        return -1
    fi
	if ! hadoop_checksize "$hadoop_task_output" "$hadoop_task_output_size"; then
        return -1
	fi
	return 0
}


function size_verify() {
    old_data_size=$1
    new_data_size=$2
    lower_threshold=$3
    upper_threshold=$4

    diff_ratio=`echo "${new_data_size}/${old_data_size}"| bc -l`
    overflow=`echo "${diff_ratio}>${upper_threshold}"| bc`
    underflow=`echo "${diff_ratio}<${lower_threshold}"| bc`

    if [ "$overflow" != 0 -o "$underflow" != 0 ]; then
        return 1
    fi
    return 0
}


## 保留最近max_file_num天的数据
function check_and_del_date() {
    [[ $# < 2 ]] && return -1
    local max_file_num=$1
    local out_path="$2"
    local dir_path="${3:-${out_path%/*}}"
    ${hadoop_bin} fs -test -d $dir_path
    if [ $? -ne 0 ]; then
        return 0
    fi
    total_lines=`${hadoop_bin} fs -ls $dir_path | awk -F ' ' '{print $8}' | sort -r | wc -l`
    del_lines=`expr $total_lines - $max_file_num`
    if [ $del_lines -lt 1 ]; then
        return 0
    fi
    result=`${hadoop_bin} fs -ls $dir_path | awk -F ' ' '{print $8}' | sort -r | tail -$del_lines`
    for line in $result
    do
        if [[ "$line" == *done.txt* ]];then
            continue
        fi
        ${hadoop_bin} fs -rmr $line
    done
    return 0
}


function send_mail {
    local TITLE=$1;
    local MSG=$2;
    local TO=$3;
    local FROM="work@`hostname`"

    echo "To:${TO}
    From:${FROM}
    Subject: ${TITLE}
    Content-type:text/html;charset=gb2312

    <html>
    <body>
        <h2>${MSG}<h2>
    </body>
    </html>" \
        | /usr/lib/sendmail -t
}


function print_log() {
    local level=$1;
    local file=$2;
    local msg=$3;
    [[ -z $file || -z $msg ]] && return 1
    local time=`date +"%Y-%m-%d %H:%M:%S"`;
    echo -e "[${level}]\t[${time}]\t${msg}" >> ${file}
    return 0
}


## 报警短信和邮件
function alarm() {
    local msg=$1;
    local title=$2
    local mail_list=$3;
    local phoneid_list=$4;

    [[ -z $mail_list ]] && return 1
	local time=`date +"%Y-%m-%d %H:%M:%S"`
    local info="`hostname`:${title} [`date +%T`]"
    echo "[$time]\t$msg" | mail -s "$info" "${mail_list}"

    [[ -z ${phoneid_list} ]] && return 2
    for phoneid in ${phoneid_list}
    do
       gsmsend -s emp01.baidu.com:15001 ${phoneid}@"${info}"
    done
}

