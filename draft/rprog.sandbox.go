package draft

type SandboxAnalyzer interface {
	PrepareSandbox()
	GetSandbox() Analyzer
	CommitSandbox()
}

type SandboxForMimicRaftKernelAnalyzer struct {
	base    *MimicRaftKernelAnalyzer
	sandbox *MimicRaftKernelAnalyzer
}

func (sbx *SandboxForMimicRaftKernelAnalyzer) PrepareSandbox() {
	sbx.sandbox = CloneMimicRaftKernelAnalyzer(sbx.base)
}

func (sbx *SandboxForMimicRaftKernelAnalyzer) GetSandbox() Analyzer {
	return sbx.sandbox
}

func (sbx *SandboxForMimicRaftKernelAnalyzer) CommitSandbox() {
	CopyMimicRaftKernelAnalyzer(sbx.base, sbx.sandbox)
}
