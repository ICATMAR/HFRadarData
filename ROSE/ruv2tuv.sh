#!/bin/bash
# ============================================================================
# Name        : ruv2tuv.sh
# Author      : Justino Martinez, justino@icm.csic.es
# Institution : ICATMAR / ICM
# Version     : 0.2
# Description : Checks the content of aladroc and process radial files to create tuv
#
# Mandatory parameters: None
#
# Optional parameters: None
#
# Variables are hardcoded at the beginning of this script. Change it at your own risk
#
# History:
#
#	-0.2 	March, 2023	Changes in procedure to avoid temporal holes
# 	-0.1	March, 2023  	Original version 
# ============================================================================
#
# VARIABLES TO BE CHANGED
# ============================================================================


# Email to receive alerts
MAIL_MESSAGES="justino@icm.csic.es"

# main folder of radials
MYFOLDER=/data/RADAR/radars/

# folder hosting the chain subfolders
CHAINFOLDER=/data/RADAR/protochain/

# log and lock files
TMPFOLDER=${CHAINFOLDER}tmp/ # where "diag" files are temporary stored (log files are deleted from this folder)
LOGFILE=${TMPFOLDER}ruv2tuv_log.txt
LOCKFILE=${CHAINFOLDER}ruv2tuv.lck

# program that generates the tuv file
# it expects input ruv files amd output tuv filename
EXECP=${CHAINFOLDER}merge_radials_chain.py

# Destination folder 
TUVFOLDER=${MYFOLDER}ROSE/L3/L3A_LS/


# where the new radial files are stored  (rsync for NFS)
FOLDERCREU=${MYFOLDER}creu/radials/measpattern/
FOLDERBEGU=${MYFOLDER}begu/radials/idealpattern/

ALADROC_PORT=33001
ALADROC_SERVER="hfradar@161.111.181.199"

# Rsync to lustre
RSYNC_LUSTRE="/mnt/lustre/repos/CSM/RADAR/ROSE/L3/L3A_LS/"
# Rsync to local
RSYNC_LOCAL="/data/RADAR"

# GITHUB on mandragora
GITHUB="/data/lustre/radar/TUVS/HFRadarData/"
GITHUB_TUV=${GITHUB}"ROSE/"
GITHUB_CREU=${GITHUB}"CREU/"
GITHUB_BEGU=${GITHUB}"BEGU/"


# First TUV to check existence
START_TUV="TOTL_ROSE_2023_03_02_1100.tuv"


# END VARIABLES
# ============================================================================
StopOnWarning=0

# ---------------- ERROR CODES
I_MESSAGE=0
W_MESSAGE=1
E_MESSAGE=2
SUCCESS_OK=0

E_DONT_EXISTS=10
E_CANT_READ=11
E_CANT_WRITE=12
E_CANT_MOVE=13
E_CANT_EXECUTE=14
E_CANT_DELETE=15
E_CANT_CREATE=16
E_CANT_UNZIP=21
E_PARAMETERS=31
E_CONFIG=32
E_UNEXPECTED=50
E_RSYNC=55

# ------------------------------------------- 
# Write messages
#       Parameters class[0-info;1-warning;2-error],message,exit number
# ------------------------------------------- 
function message(){

        local code
        if [ $1 -eq ${I_MESSAGE} ]; then code=I; fi
        if [ $1 -eq ${W_MESSAGE} ]; then code=W; fi
        if [ $1 -eq ${E_MESSAGE} ]; then code=E; fi

        local MS=`date +"%N"`
        local DATETIME=`date +"%Y-%m-%d %H:%M:%S"`
        DATETIME=${DATETIME}"."${MS:0:3}

        IDPROG=$(printf "%08d" $$)
        echo  -e "${DATETIME}\t[$IDPROG]\t[${code}]\t$2"

        if [ $1 -eq ${W_MESSAGE} ]; then
                # ---------------------------------------------------- Send Mail
                if [ "${MAIL_MESSAGES}" != "NONE" ]; then
                        #echo -e "Warning message:"$2 | /bin/mail ${MAIL_MESSAGES} -s "${ChainName} production chain warning"
			echo "Subject: $2" | /usr/sbin/sendmail ${MAIL_MESSAGES}
                fi
                # ----------------------------------------------------
                if [ ${StopOnWarning} -eq 1 ]; then
                        echo  -e "${DATETIME}\t[$IDPROG]\t[I]\tExecution finished with warnings (StopOnWarning set to 1)"
                        exit $3;
                fi
        fi
        if [ $1 -eq ${E_MESSAGE} ]; then
                # ---------------------------------------------------- Send Mail
                if [ "${MAIL_MESSAGES}" != "NONE" ]; then
                        #echo -e "Error message:"$2"\nPROCESS STOPPED" | /bin/mail ${MAIL_MESSAGES} -s "${ChainName} production chain error"
		 	echo "Subject: $2" | /usr/sbin/sendmail ${MAIL_MESSAGES}

                fi
                # ----------------------------------------------------
                echo  -e "${DATETIME}\t[$IDPROG]\t[I]\tExecution finished with errors"
                exit $3;
        fi
}


# ------------------------------------------- 
# Get BEGU and CREU list of files from aladroc
# ------------------------------------------- 
function getRUV(){
	message ${I_MESSAGE} "Checking RUV files list" ${SUCCESS_OK} >> ${LOGFILE}
	# Get files from begu and creu
	LIST_BEGU=$(ssh -p ${ALADROC_PORT} ${ALADROC_SERVER} 'ls /home/hfradar/radars/begu/radials/idealpattern/*ruv 2>/dev/null | tr "\n" " "')
	LIST_CREU=$(ssh -p ${ALADROC_PORT} ${ALADROC_SERVER} 'ls /home/hfradar/radars/creu/radials/measpattern/*ruv 2>/dev/null | tr "\n" " "')
	if [[ ${LIST_CREU} == "" ]] || [[ ${LIST_BEGU} == "" ]]; then
        	mesg="No ruv files found in CREU or BEGU!"
        	message ${E_MESSAGE} "${mesg}" ${E_DONT_EXISTS} >> ${LOGFILE}
	fi
	IFS=' ' read -r -a IN_SERVER_BEGU <<< "${LIST_BEGU}"
	IFS=' ' read -r -a IN_SERVER_CREU <<< "${LIST_CREU}"

	let LENBEGU=${#IN_SERVER_BEGU[@]}-1
	let LENCREU=${#IN_SERVER_CREU[@]}-1
        message ${I_MESSAGE} "Found ${LENBEGU} BEGU files and ${LENCREU} CREU files" ${SUCCESS_OK} >> ${LOGFILE}

}

# -------------------------------------------
# Get TUV list of files from local folder
# -------------------------------------------
function getTUV(){
	message ${I_MESSAGE} "Checking TUV files list" ${SUCCESS_OK} >> ${LOGFILE}
        # Get files from begu and creu
        LIST_TUV=$(ls ${TUVFOLDER}*tuv 2>/dev/null | tr "\n" " ")
        IFS=' ' read -r -a IN_LOCAL_TUV <<< "${LIST_TUV}"

        let LENTUV=${#IN_LOCAL_TUV[@]}-1
        message ${I_MESSAGE} "Found ${LENTUV} TUV files" ${SUCCESS_OK} >> ${LOGFILE}

}

# -------------------------------------------
# Checks if an element is included in an array
# -------------------------------------------
containsElement () {
  local e match="$1"
  shift
  let i=0
  for e; do [[ "$e" == *"$match"* ]] && return $i; let i=$i+1;done
  return 255
}


#===========================================
# main
#===========================================

# if a lock file exists finish silently. 
if [ -f ${LOCKFILE} ]; then
	exit
fi

#create the lock file to stop any further execution until this one finishes
touch ${LOCKFILE} &> /dev/null

message ${I_MESSAGE} "Starting execution" ${SUCCESS_OK} > ${LOGFILE}

getRUV

LAST_SERVER_BEGU_DATE=$(echo "${IN_SERVER_BEGU[-1]}" | awk -F"_" '{print $(NF-3)$(NF-2)$(NF-1)substr($NF,0,4)}')
LAST_SERVER_CREU_DATE=$(echo "${IN_SERVER_CREU[-1]}" | awk -F"_" '{print $(NF-3)$(NF-2)$(NF-1)substr($NF,0,4)}')

# Get last files in local folder
LAST_LOCAL_BEGU_DATE=$(ls -tr ${FOLDERBEGU}*ruv | tail -1 | awk -F"_" '{print $(NF-3)$(NF-2)$(NF-1)substr($NF,0,4)}')
LAST_LOCAL_CREU_DATE=$(ls -tr ${FOLDERCREU}*ruv | tail -1 | awk -F"_" '{print $(NF-3)$(NF-2)$(NF-1)substr($NF,0,4)}')

# perform rsync
if [[ ${LAST_LOCAL_BEGU_DATE} != ${LAST_SERVER_BEGU_DATE} ]] || [[ ${LAST_LOCAL_CREU_DATE} != ${LAST_SERVER_CREU_DATE} ]]; then
	# lustre
	if [[ ${RSYNC_LUSTRE} != "" ]]; then
		message ${I_MESSAGE} "Starting lustre rsync" ${SUCCESS_OK} >> ${LOGFILE}

	      	STATUS=$(ssh dataadmin@mandragora 'rsync -a -e  "ssh -p 33001" hfradar@161.111.181.199:/home/hfradar/radars /mnt/lustre/repos/CSM/RADAR >/dev/null')
	        if [[ $? != 0 ]]; then
			rm ${LOCKFILE} &> /dev/null	
			mesg="Error in lustre rsync"
		       	message ${E_MESSAGE} "${mesg}" ${E_RSYNC} >> ${LOGFILE}
                fi
	fi
	# local (NFS)
	if [[ ${RSYNC_LOCAL} != "" ]]; then
	        message ${I_MESSAGE} "Starting local rsync" ${SUCCESS_OK} >> ${LOGFILE}	
	      	STATUS=$(rsync -a -e "ssh -p ${ALADROC_PORT}" ${ALADROC_SERVER}:/home/hfradar/radars ${RSYNC_LOCAL}  > /dev/null)
        	if [[ $? != 0 ]]; then
			rm ${LOCKFILE} &> /dev/null
	                mesg="Error in local rsync"
        	        message ${E_MESSAGE} "${mesg}" ${E_RSYNC} >> ${LOGFILE}
	        fi
	fi
	# refresh list of files
	getRUV
fi
# Get the name of the tuv files of ROSE (we assume that it has been correctly generated)
getTUV

LAST_DATE=$(echo ${START_TUV} | awk -F"_" '{print $(NF-3)$(NF-2)$(NF-1)substr($NF,0,4)}')
let MAPS=0
let ERROR_MAPS=0
for BEGURUV in "${IN_SERVER_BEGU[@]}"; do
	CURDATE=$(echo ${BEGURUV} | awk -F"_" '{print $(NF-3)$(NF-2)$(NF-1)substr($NF,0,4)}')
	CURDATESTR=$(echo ${BEGURUV} | awk -F"_" '{print $(NF-3)"_"$(NF-2)"_"$(NF-1)"_"substr($NF,0,4)}')
	if [[ ${CURDATE} > ${LAST_DATE} ]]; then
		echo ${CURDATE}" "${LAST_DATE}
		containsElement "${CURDATESTR}" "${IN_LOCAL_TUV[@]}"
		if [[ $? == 255 ]]; then # TUV Do not exists
			containsElement "${CURDATESTR}" "${IN_SERVER_CREU[@]}"
			if [[ $? < 255 ]]; then # CREU file exists
				message ${I_MESSAGE} "Creating TUV for ${CURDATE}" ${SUCCESS_OK} >> ${LOGFILE}
				CR=$(/usr/bin/basename ${IN_SERVER_CREU[$i]})
				BE=$(/usr/bin/basename ${BEGURUV})
			
				inputCREU=${FOLDERCREU}${CR}
				inputBEGU=${FOLDERBEGU}${BE}

				GENDATE=$(echo ${BE} | awk -F"_" '{print $(NF-3)"_"$(NF-2)"_"$(NF-1)"_"$NF}' | sed -e 's|ruv$|tuv|')
				OUT_TUV=${TUVFOLDER}${START_TUV:0:-19}${GENDATE}
				TU=$(/usr/bin/basename ${OUT_TUV})

				# redirect output of python to a temporary "log" file
				FIL=$(/usr/bin/basename -s tuv ${OUT_TUV})
				DIR=$(/usr/bin/dirname ${OUT_TUV})
				DIAG=${TMPFOLDER}${FIL}log
				message ${I_MESSAGE} "Redirecting output from merge_radials to ${DIAG}" ${SUCCESS_OK} >> ${LOGFILE}
				STATUS= $(${EXECP} ${inputCREU} ${inputBEGU} ${OUT_TUV} &> ${DIAG})
				if [[ $? != 0 ]]; then
					mesg="Error executing merge_radials for ${GENDATE}"
					message ${W_MESSAGE} "${mesg}" ${E_UNEXPECTED} >> ${LOGFILE}
					#echo "Subject: $mesg" | /usr/sbin/sendmail ${MAIL_MESSAGES}	
					let ERROR_MAPS=${ERROR_MAPS}+1
				else
					let MAPS=${MAPS}+1
					if [[ ${GITHUB} != "" ]]; then
						UPDATE_FILES=""
					        STATUS=$(scp ${OUT_TUV} justino@mandragora:${GITHUB_TUV})
						if [[ $? != 0 ]]; then
                                       	                mesg="${OUT_TUV} cannot be copied to ${GITHUB_TUV}"
                                               	        message ${W_MESSAGE} "${mesg}" ${E_UNEXPECTED} >> ${LOGFILE}
						else 
							UPDATE_FILES="ROSE/"${TU}
						fi

				                STATUS=$(scp ${inputCREU} justino@mandragora:${GITHUB_CREU})
						if [[ $? != 0 ]]; then
                                       	                mesg="${inputCREU} cannot be copied to lustre ${GITHUB_CREU}"
                                               	        message ${W_MESSAGE} "${mesg}" ${E_UNEXPECTED} >> ${LOGFILE}
						else 
							UPDATE_FILES=${UPDATE_FILES}" CREU/"${CR}
						fi

               					STATUS=$(scp ${inputBEGU} justino@mandragora:${GITHUB_BEGU})
						if [[ $? != 0 ]]; then
                                       	                mesg="${inputBEGU} cannot be copied to lustre ${GITHUB_BEGU}"
                                               	        message ${W_MESSAGE} "${mesg}" ${E_UNEXPECTED} >> ${LOGFILE}
						else
       	                                                UPDATE_FILES=${UPDATE_FILES}" BEGU/"${BE}
               	                                fi

						if [[ ${UPDATE_FILES} != "" ]]; then
							message ${I_MESSAGE} "Updating github... ${UPDATE_FILES}" ${SUCCESS_OK}  >> ${LOGFILE}
							STATUS=$(ssh justino@mandragora "/data/lustre/radar/TUVS/update.sh ${UPDATE_FILES} &>/dev/null")
               						if [[ $? != 0 ]]; then
					                        mesg="Error updating github"
				        	                rm ${LOCKFILE} &>/dev/null
				                	        message ${E_MESSAGE} "${mesg}" ${E_UNEXPECTED} >> ${LOGFILE}
					                fi
						fi
						
					fi
		
					if [[ ${RSYNC_LUSTRE} != "" ]]; then
						STATUS=$(scp ${OUT_TUV} dataadmin@mandragora:${RSYNC_LUSTRE})
						if [[ $? != 0 ]]; then
							mesg="File cannot be copied to lustre"
							message ${W_MESSAGE} "${mesg}" ${E_UNEXPECTED} >> ${LOGFILE}
						fi
					fi
				fi	
			fi
		fi
	        LAST_DATE=${CURDATE}
	fi
done
message ${I_MESSAGE} "Created ${MAPS} TUV files" ${SUCCESS_OK} >> ${LOGFILE}
if [[ ${ERROR_MAPS} > 0 ]]; then
	message ${I_MESSAGE} "${ERROR_MAPS} TUV files with errors" ${SUCCESS_OK} >> ${LOGFILE}
fi
if [[ ${MAPS} == 0 ]]; then
	mesg="No TUV files have been created"
	rm ${LOCKFILE} &>/dev/null
        message ${W_MESSAGE} "${mesg}" ${E_UNEXPECTED} >> ${LOGFILE}
fi

message ${I_MESSAGE} "Process successfully finished" ${SUCCESS_OK} >> ${LOGFILE}
rm ${LOCKFILE} &>/dev/null


## prepare and send email
#STATUS=$(/usr/bin/uuencode ${TMPFOLDER}${FILETUV} ${FILETUV} > /tmp/file.uue)
#
#export MAILTO="jisern@icm.csic.es"
#export CONTENT="/tmp/file.uue"
#export SUBJECT="Last TUV file"
#(
# echo "Subject: $SUBJECT"
# echo "MIME-Version: 1.0"
# echo "Content-Type: application"
# echo "Content-Transfer-Encoding: uuencode"
# echo "Content-Disposition: attachment; filename*=utf-8''${FILETUV}"
# cat $CONTENT
#) | /usr/sbin/sendmail $MAILTO

