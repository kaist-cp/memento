add_llvm_loadable_module( RollablePasses
    MemoryIdempotenceAnalysis.cpp;NaiveHook.cpp;ClobberFunc.cpp;GlobalVal.cpp;
    DEPENDS
    intrinsics_gen
    PLUGIN_TOOL
    opt
)
