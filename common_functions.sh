#!/bin/bash

MailTo="zhangzhuyu@baidu.com"
InfoTo="13718850570"

function msg()
{
    [ $# -lt 2 ] && return 1
    local RET=$1
    shift
    local DATE=$(date +"%Y-%m-%d %T")
    LEVEL_INFO="[ERROR]"
    EXIT_CMD=exit
    if [ "$RET" = "0" ] ; then
        LEVEL_INFO="[INFO]"
        EXIT_CMD=return
    fi
    echo "${DATE} ${LEVEL_INFO} $@"
    $EXIT_CMD $RET
}

function send_mail()
{
    [ $# -lt 3 ] && return 1
    local RET=$1
    shift
    local DATE=$(date +"%Y-%m-%d %T")
    LEVEL_INFO="[ERROR]"
    EXIT_CMD=exit
    if [ "$RET" = "0" ] ; then
        LEVEL_INFO="[INFO]"
        EXIT_CMD=return
    fi
    TITLE_INFO="${LEVEL_INFO} $1"
    shift
    echo "${DATE} ${LEVEL_INFO} $@" | mail -s "${TITLE_INFO}" $MailTo
    $EXIT_CMD $RET
}

function send_msg()
{
    [ $# -lt 2 ] && return 1
    local RET=$1
    shift
    local DATE=$(date +"%m-%d %H:%M")
    LEVEL_INFO="[WARN]"
    if [ "$RET" = "0" ]; then
        LEVEL_INFO="[INFO]"
    elif [ "$RET" = "2" ];then
        LEVEL_INFO="[FATAL]"
    fi
    /bin/gsmsend -s emp01.baidu.com:15002 ${InfoTo}@"${LEVEL_INFO} $@ [${DATE}]"
    return $RET
}

function get_date()
{
    local diff_days="0"
    local fmt="%Y%m%d"
    if [ $# -ge 1 ] ; then
        diff_days=$1
    fi
    if [ $# -ge 2 ] ; then
        fmt=$2
    fi
    echo $(date -d "$diff_days day" "+$fmt")
}

function get_next_date()
{
    [ $# -lt 1 ] && return 1
    local diff_days="1"
    local curr_date=$1
    local fmt="%Y%m%d"
    if [ $# -ge 2 ] ; then
        diff_days=$2
    fi
    if [ $# -ge 3 ] ; then
        fmt=$3
    fi
    echo $(date -d "$curr_date $diff_days day" "+$fmt")
}

function get_last_date()
{
    [ $# -lt 1 ] && return 1
    local diff_days="1"
    local curr_date=$1
    local fmt="%Y%m%d"
    if [ $# -ge 2 ] ; then
        diff_days=$2
    fi
    if [ $# -ge 3 ] ; then
        fmt=$3
    fi
    echo $(date -d "$curr_date $diff_days day ago" "+$fmt")
}

function get_files()
{
    [ $# -lt 2 ] && return 1
    local dir=$1
    local pattern=$2
    local cnt="1"
    if [ $# -ge 3 ]; then
        cnt=$3
    fi
    if [ ! -d $dir ] || [ -z $pattern ]; then
        echo "param error"
        return 1
    fi
    local file_list=`ls $dir | grep $pattern | sort -r | head "-$cnt"`
    echo $file_list
}

function cleardir()
{
    if [ $# -ne 1 ]; then
        echo "param error"
        return 1
    fi
    local dir=$1
    if [ -d $dir ]; then
        rm -rf $dir
    fi
    mkdir -p $dir
    return 0
}

function get_file_size()
{
    if [ $# -ne 1 ]; then
        echo "param error"
        return 1
    fi
    local file=$1
    if [ ! -f $file ]; then
        echo "param error"
        return 1
    fi
    local ret=`ls -al $file | awk -v FS=" " '{print $5}'`
    echo $ret
    return 0
}

function check_md5()
{
    if [ $# -ne 1 ];then
        echo "usage: check_md5 filename"
        return 1
    fi
    filename=$1
    if [ ! -f $filename ] || [ ! -f ${filename}.md5 ];then
        return 1
    fi
    local flag1=`md5sum $filename | awk '{print $1}'`
    local flag2=`head -1 "${filename}.md5" | awk '{print $1}'`
    if [ "x$flag1" == "x$flag2" ];then
        return 0
    else
        return 1
    fi
}

function check_file()
{
    if [ $# -ne 2 ];then
        echo "usage: check_file old_file new_file"
        return 1
    fi
    if [ ! -f $2 ];then
        echo "new_file $2 does not exist!"
        return 2
    fi
    local cnt1=0
    if [ -f $1 ];then
        cnt1=`cat $1 | wc -l`
    fi
    local cnt2=`cat $2 | wc -l`
    local old_cnt=`expr $cnt1 \* 75`
    local new_cnt=`expr $cnt2 \* 100`
    if [ $new_cnt -lt $old_cnt ];then
        return 1
    else
        return 0
    fi
}

function PRINT_LOG()
{
    if [ $# -lt 4 ];then
        echo "usage: PRINT_LOG ret step header message"
        return 1
    fi
    local RET=$1
    shift
    local STEP=$1
    shift
    local HEADER=$1
    shift
    local PROCESS=$@
    if [ $RET -ne 0 ];then
        echo "step${STEP}: $PROCESS failed!" | mail -s "[FATAL]: ${HEADER}:${PROCESS} failed!" $MailTo
        msg 2 "step${STEP}: $PROCESS failed!"
    else
        msg 0 "step${STEP}: $PROCESS succeed!"
    fi
}

function get_donefile_value()
{
    if [ $# -ne 2 ];then
        echo "usage: get_donefile_value \"line\" key"
        return 1
    fi
    local LINE=$1
    local KEY=".$2"
    local value=`echo "${LINE}" | ~/.jumbo/bin/jq "${KEY}"`
    value=${value//\"}
    echo $value
}

# para  start_date end_date
function gen_daterange_start_end()
{
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

# para end_date data_len
function gen_daterange_end_len()
{
    [[ $# < 2 ]] && return -1
    local end_date=`date --date="$1" +%Y%m%d`
    local date_len=$2
    local dem=${3:-","}
    #local sd=`date -d "$start_date" +%Y-%m-%d`
    #local ed=`date -d "$end_date" +%Y-%m-%d`
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

# para end_date data_len
function get_startdate_by_end_len()
{
    [[ $# != 2 ]] && return -1
    local end_date=`date --date="$1" +%Y-%m-%d`
    local days=$2
    if [[ $days < 0 ]]
    then
        let "days=-$days-1"
    else
        let "days=-$days+1"
    fi
    date -d "$end_date $days day " +%Y%m%d
}

function wget_file()
{
    local RET=0
    FTP_DIR=$1
    FILENAME=$2
    if [ -f "${FILENAME}" ];then
        mv -f ${FILENAME} ${FILENAME}_pre
    fi
    wget ${FTP_DIR} -O ${FILENAME}
    check_file "${FILENAME}_pre" "${FILENAME}"
    if [ $? -ne 0 ];then
        if [ -f "${FILENAME}_pre" ];then
            mv -f ${FILENAME}_pre ${FILENAME}
            echo "[ERROR]: ${FILENAME} is less than old!"
            RET=1
        else
            echo "[ERROR]: ${FILENAME} update failed!"
            RET=2
        fi
    fi
    return $RET
}

function get_slide_window_by_pattern()
{
    if [ $# -lt 4 ];then
        echo "usage: get_slide_window hadoop hdfs_path day_len pattern1 [pattern2]"
        return 1
    fi
    local DATE=`date +"%Y%m%d"`
    local HADOOP=$1
    local HDFS_PATH=$2
    local DAY_LEN=$3
    local PATTERN1=$4
    local PATTERN2=""
    if [ $# -ge 5 ]; then
        local PATTERN2=$5
    fi
    
    local total_files=`$HADOOP fs -ls $HDFS_PATH | awk -v FS="/" '{print $NF}' | grep "$PATTERN1" | grep "$PATTERN2" | sort -nr`
    local total_num=`echo $total_files | awk '{print NF}'`
    local del_num=`expr $total_num - $DAY_LEN`
    local day_count=0
    echo "========= get_slide_window [$DATE] ============"
    if [ $del_num -ge 1 ];then
        for date in `echo $total_files`;do
            let "day_count++"
            if [ $day_count -gt $DAY_LEN ];then
                echo "$HADOOP fs -rmr ${HDFS_PATH}/$date"
                $HADOOP fs -rmr "${HDFS_PATH}/$date"
            fi
        done
    fi
    echo "==============================================="
    return 0
}

function get_slide_window()
{
    if [ $# -lt 3 ];then
        echo "usage: get_slide_window hadoop hdfs_path day_len"
        return 1
    fi
    local DATE=`date +"%Y%m%d"`
    local HADOOP=$1
    local HDFS_PATH=$2
    local DAY_LEN=$3
    local total_files=`$HADOOP fs -ls $HDFS_PATH | awk -v FS="/" '{print $NF}' | grep "^20[0-9]\{6\}$" | sort -nr`
    local total_num=`echo $total_files | awk '{print NF}'`
    local del_num=`expr $total_num - $DAY_LEN`
    local day_count=0
    echo "========= get_slide_window [$DATE] ============"
    if [ $del_num -ge 1 ];then
        for date in `echo $total_files`;do
            let "day_count++"
            if [ $day_count -gt $DAY_LEN ];then
                echo "$HADOOP fs -rmr ${HDFS_PATH}/$date"
                $HADOOP fs -rmr "${HDFS_PATH}/$date"
            fi
        done
    fi
    echo "==============================================="
    return 0
}

