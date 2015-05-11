# display usage message
function usage() {
	cat <<EndUsageText

Usage: `basename $0` DB_FOLDER

	This script pulls wqx data exports from the EPA.
		
DB_FOLDER
	One of these must be specified. If more than one is set, the last one parsed will win.
			
	cinolog   download to the ci environment
	devnolog  download to the dev environment
	qanolog   download to the qa environment
	dbnolog   download to the production environment
		
EndUsageText
}

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

# if not exactly one parameter, display usage and quit
[ "$#" -ne 1 ] && usage && stop_bad

# parse arguments
for arg in "$@"
do
	case $arg in
		cinolog)
			DB_FOLDER=$arg
			;;
		devnolog)
			DB_FOLDER=$arg
			;;
		qanolog)
			DB_FOLDER=$arg
			;;
		dbnolog)
			DB_FOLDER=$arg
			;;
	esac
done

# if any required variables are null or empty, display usage and quit
[ ! -n "${DB_FOLDER}" ] && usage && stop_bad

WORK_DIR=/mnt/wqp_data/${DB_FOLDER}
EXPORT_REF="owpub_vmwaters1_wqx_Weekly_expdp.ref"
EXPORT_LOG="owpub_vmwaters1_wqx_Weekly_expdp.log"
DUMP_FILE_GREP="owpub_vmwaters1_wqx_Weekly_...cdmp"
CLEAN_UP_GREP=".*_wqx_Weekly_...cdmp"

starting_dir=`pwd`

cd ${WORK_DIR}

mv ${EXPORT_LOG} ${EXPORT_REF}

cd ${starting_dir}