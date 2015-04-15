#!/bin/bash
DATE_SUFFIX=`date +%Y%m%d_%H%M`
HTTP_BASE=http://www.epa.gov/storet/download/storetw
WORK_DIR=/pdc/wqp_data

# output of this script is parsed and looks for this text to raise errors
function stop_bad() {
	echo "Script generated an error, quitting."
	exit 1
}

# output of this script is parsed and looks for this text to not run other steps
function stop_ok() {
	echo "No new export to process."
	exit 0
}

# set so if any command in a piped sequence returns a non-zero error code, the script fails
set -o pipefail

EXPORT_REF="owpub_vmwaters1_wqx_Weekly_expdp.ref"
EXPORT_LOG="owpub_vmwaters1_wqx_Weekly_expdp.log"
DUMP_FILE_GREP="owpub_vmwaters1_wqx_Weekly_...cdmp"
CLEAN_UP_GREP=".*_wqx_Weekly_...cdmp"

cd ${WORK_DIR}

# quietly pull the export log, using timestamping to only pull if remote file is newer than local
wget -Nq ${HTTP_BASE}/${EXPORT_LOG}

# check export log for the phrase 'successfully completed'; if not found, quit
grep -q "successfully completed" ${EXPORT_LOG} || (echo "${EXPORT_LOG} does not contain 'successfully completed'." && stop_bad)

# if a reference file exists and the log isn't newer than the reference file, we don't need to do more
([ -f ${EXPORT_REF} ] && [ ! ${EXPORT_LOG} -nt ${EXPORT_REF} ]) && stop_ok

# remove any dump files of the same export type found locally but not in the export log
comm -13 <(grep -o ${DUMP_FILE_GREP} ${EXPORT_LOG}) <(ls | grep ${CLEAN_UP_GREP}) | xargs rm -f

# download any dump files newer on remote than they are on local
grep -o ${DUMP_FILE_GREP} ${EXPORT_LOG} | sed -e 's/^/http:\/\/www.epa.gov\/storet\/download\/storetw\//' | xargs -n 1 -P 12 wget -Nq