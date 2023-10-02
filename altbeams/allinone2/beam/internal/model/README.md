Code copied from https://github.com/apache/beam/tree/master/sdks/go/pkg/beam/model

This will work for now, but will likely lead to conflicts if both this prototype
and the current SDK are built into the same binary, per
https://protobuf.dev/reference/go/faq/#namespace-conflict.

In any migration plan, the Old version will depend on the New version, which will
and the protos canonical location will remain.