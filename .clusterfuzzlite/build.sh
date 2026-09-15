#!/bin/bash -eu
#
# Copyright 2026 The OpenNMS Group, Inc.
# SPDX-License-Identifier: Apache-2.0
#
# Created by Ronny Trommer <ronny@opennms.com>
#
# Build the fuzz targets for ClusterFuzzLite.
#
# The targets live in the plugin's test sources, so the ordinary build keeps
# them compiling and `make test` keeps their invariants honest. Here they are
# compiled once more with their dependencies and wrapped in the launcher
# script the fuzzing runner expects, one per target.

cd "$SRC/prometheus-remote-writer"

# Test scope, because that is where the targets are. Tests themselves are
# skipped: this is a build, and the fuzzer is the thing that runs them.
./mvnw -q -B -ntp -pl plugin -am test-compile -DskipTests

# Everything the plugin needs at runtime, next to the classes that use it.
./mvnw -q -B -ntp -pl plugin dependency:copy-dependencies \
    -DincludeScope=runtime -DoutputDirectory="$OUT/lib"

mkdir -p "$OUT/classes"
cp -r plugin/target/classes/. "$OUT/classes/"
cp -r plugin/target/test-classes/. "$OUT/classes/"

# The JDK the targets were compiled for: the base image's is 17 and this is
# 21 bytecode, so the runner needs this one rather than its own.
cp -r "$JAVA_HOME" "$OUT/jdk"

RUNTIME_CLASSPATH="\$this_dir/classes:\$this_dir/lib/*:\$this_dir"

for target in FrameFuzzer SanitizerFuzzer WalEntryCodecFuzzer; do
  class="org.opennms.plugins.prometheus.remotewriter.fuzz.$target"
  cat > "$OUT/$target" <<EOF
#!/bin/bash
# LLVMFuzzerTestOneInput: the string the fuzzing infrastructure greps for to
# recognise this as a fuzz target.
this_dir=\$(dirname "\$0")
if [[ "\$@" =~ (^| )-runs=[0-9]+(\$| ) ]]; then
  mem_settings='-Xmx1900m:-Xss900k'
else
  mem_settings='-Xmx2048m:-Xss1024k'
fi
JAVA_HOME="\$this_dir/jdk" \\
LD_LIBRARY_PATH="\$this_dir/jdk/lib/server":\$this_dir \\
"\$this_dir/jazzer_driver" \\
  --agent_path="\$this_dir/jazzer_agent_deploy.jar" \\
  --cp="$RUNTIME_CLASSPATH" \\
  --target_class="$class" \\
  --jvm_args="\$mem_settings" \\
  "\$@"
EOF
  chmod +x "$OUT/$target"
done
