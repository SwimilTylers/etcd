package draft

type SandboxAnalyzer interface {
	PrepareSandbox()
	GetSandbox() Analyzer
	CommitSandbox()
}

type sbxMimicRaftKernelAnalyzer struct {
	base    *MimicRaftKernelAnalyzer
	sandbox *MimicRaftKernelAnalyzer
}

func NewSandboxForMimicRaftKernelAnalyzer(an *MimicRaftKernelAnalyzer) SandboxAnalyzer {
	return &sbxMimicRaftKernelAnalyzer{base: an}
}

func (sbx *sbxMimicRaftKernelAnalyzer) PrepareSandbox() {
	sbx.sandbox = CloneMimicRaftKernelAnalyzer(sbx.base)
}

func (sbx *sbxMimicRaftKernelAnalyzer) GetSandbox() Analyzer {
	return sbx.sandbox
}

func (sbx *sbxMimicRaftKernelAnalyzer) CommitSandbox() {
	CopyMimicRaftKernelAnalyzer(sbx.base, sbx.sandbox)
}
