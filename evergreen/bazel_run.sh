# Usage:
#   bazel_compile [arguments]
#
# Required environment variables:
# * ${target} - Build target
# * ${args} - Extra command line args to pass to "bazel run"

# Needed for evergreen scripts that use evergreen expansions and utility methods.
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" > /dev/null 2>&1 && pwd)"
. "$DIR/prelude.sh"

cd src

set -o errexit
set -o verbose

activate_venv

# Use `eval` to force evaluation of the environment variables in the echo statement:
eval echo "Execution environment: Args: ${args} Target: ${target}"

source ./evergreen/bazel_RBE_supported.sh

if bazel_rbe_supported; then
  LOCAL_ARG=""
else
  LOCAL_ARG="--config=local"
fi

ARCH=$(uname -m)
if [[ "$ARCH" == "arm64" || "$ARCH" == "aarch64" ]]; then
  ARCH="arm64"
elif [[ "$ARCH" == "ppc64le" || "$ARCH" == "ppc64" || "$ARCH" == "ppc" || "$ARCH" == "ppcle" ]]; then
  ARCH="ppc64le"
elif [[ "$ARCH" == "s390x" || "$ARCH" == "s390" ]]; then
  ARCH="s390x"
else
  ARCH="amd64"
fi

# TODO(SERVER-86050): remove the branch once bazelisk is built on s390x & ppc64le
if [[ $ARCH == "ppc64le" ]] || [[ $ARCH == "s390x" ]]; then
  BAZEL_BINARY=$TMPDIR/bazel
else
  BAZEL_BINARY=$TMPDIR/bazelisk
fi

# Set the JAVA_HOME directories for ppc64le and s390x since their bazel binaries are not compiled with a built-in JDK.
if [[ $ARCH == "ppc64le" ]]; then
  export JAVA_HOME="/usr/lib/jvm/java-21-openjdk"
elif [[ $ARCH == "s390x" ]]; then
  export JAVA_HOME="/usr/lib/jvm/java-21-openjdk"
fi

# AL2 stores certs in a nonstandard location
if [[ -f /etc/os-release ]]; then
  DISTRO=$(awk -F '[="]*' '/^PRETTY_NAME/ { print $2 }' < /etc/os-release)
  if [[ $DISTRO == "Amazon Linux 2" ]]; then
    export SSL_CERT_DIR=/etc/pki/tls/certs
    export SSL_CERT_FILE=/etc/pki/tls/certs/ca-bundle.crt
  elif [[ $DISTRO == "Red Hat Enterprise Linux"* ]]; then
    export SSL_CERT_DIR=/etc/pki/ca-trust/extracted/pem
    export SSL_CERT_FILE=/etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem
  fi
fi

# Print command being run to file that can be uploaded
echo "python buildscripts/install_bazel.py" > bazel-invocation.txt
echo "bazel run --verbose_failures $LOCAL_ARG ${args} ${target}" >> bazel-invocation.txt

# Run bazel command, retrying up to five times
MAX_ATTEMPTS=5
for ((i = 1; i <= $MAX_ATTEMPTS; i++)); do
  eval $BAZEL_BINARY run --verbose_failures $LOCAL_ARG ${args} ${target} >> bazel_output.log 2>&1 && RET=0 && break || RET=$? && sleep 1
  if [ $i -lt $MAX_ATTEMPTS ]; then echo "Bazel failed to execute, retrying ($(($i + 1)) of $MAX_ATTEMPTS attempts)... " >> bazel_output.log 2>&1; fi
done

$python ./buildscripts/simple_report.py --test-name "bazel run ${args} ${target}" --log-file bazel_output.log --exit-code $RET
exit $RET
