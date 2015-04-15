#!/bin/bash
WORK_DIR=/pdc/wqp_data

# output of this script is parsed and looks for this text to raise errors
function stop_bad() {
	echo "Script generated an error, quitting."
	exit 1
}

EXPORT_REF="owpub_vmwaters1_wqx_Weekly_expdp.ref"
EXPORT_LOG="owpub_vmwaters1_wqx_Weekly_expdp.log"

starting_dir=`pwd`

cd ${WORK_DIR}

mv ${EXPORT_LOG} ${EXPORT_REF}

cd ${starting_dir}