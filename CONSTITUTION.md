# The Sovereign AI Constitution

> **Governance Notice:** This Constitution serves exclusively as the internal architectural charter, technical doctrine, and engineering standard for the upstream AIEN Sovereign AI Ecosystem. It does not modify, condition, restrict, or supplement the licenses granted to downstream users of AIEN software under the GNU Affero General Public License, version 3 or later (AGPL-3.0-or-later). Downstream developers possess complete operational freedom without obligation to adopt, enforce, or retain this internal charter.

## Preamble

Artificial intelligence is a shared milestone of human civilization. The breakthroughs achieved across the globe, from American laboratories building ChatGPT, Claude, and Grok, to international teams creating open-weight architectures such as Qwen and DeepSeek, reflect the dedication of human beings working toward the frontier of intelligence.

We harbor no animosity toward commercial technology companies or international research teams. We recognize that frontier models require immense capital, specialized infrastructure, and remarkable engineering talent. We celebrate these achievements and use these tools daily.

Our concern centers on opacity, isolation, and uncoordinated competition. Frontier laboratories face acute compute constraints, infrastructure bottlenecks, and intense market pressures. When organizations compete in secrecy and conceal their operational limits, they fuel an uncontrolled race that risks deploying unverified systems or erecting opaque barriers around shared knowledge. Meanwhile, independent engineers and local communities worldwide possess distributed computing power, native systems expertise, and a willingness to help, yet have remained excluded from the conversation.

This project exists to bridge that divide. We believe that humanity must unite across national, organizational, and technological boundaries. By constructing high-efficiency native runtimes, verifiable local execution engines, and hardware-bound privacy systems, we expand global compute capacity and invite cooperative problem-solving. We direct capital, local silicon, and human ingenuity toward solving real-world challenges for clinics, farmers, independent engineers, schools, and families.

This Constitution establishes our foundational principles, our engineering standards, and our covenant of global cooperation.

---

## Article I: Pillars of Global Unity and Local Empowerment

### Section 1. Enduring Stewardship Over Speculative Frenzy
1. This initiative represents a durable commitment to human advancement, community resilience, and unconditional open-source collaboration.
2. We reject speculative monetization, deceptive marketing, and get-rich-quick schemes. We build tools designed to operate reliably for decades.
3. Every tool, model adapter, and service we release runs locally on consumer and workstation hardware without requiring recurring subscriptions or remote permission handshakes.
4. We build to empower people everywhere: the local clinic, the family farm, the independent engineer, the student, and the community builder, while extending open collaboration to research organizations worldwide.

### Section 2. Shared Intelligence for All Humanity
1. Intelligence belongs to all of humanity, spanning every continent, nation, and culture.
2. We actively bridge geopolitical and institutional divides. Engineers in the United States, China, Europe, and every region of the globe share a common human identity. We honor and study open models from all regions, recognizing that mutual respect and technical cooperation advance global safety.
3. The current compute bottleneck cannot be solved in isolation. When organizations face compute shortages, the constructive response is transparency and distributed partnership rather than closed competition.
4. Local computing power decentralizes the computational burden, giving individuals the ability to think, build, analyze, and communicate freely while contributing distributed capacity to the global human collective.

### Section 3. Upstream Architectural Charter and Downstream Freedom
1. This Constitution serves as the internal architectural charter, technical doctrine, and engineering standard for the upstream AIEN Sovereign AI Ecosystem and its core repositories.
2. Downstream forks, independent applications, client libraries, and commercial distributions are governed strictly and exclusively by the terms of the GNU Affero General Public License, version 3 or later (AGPL-3.0-or-later) (see [LICENSE](LICENSE)). Downstream developers possess complete freedom to innovate, customize, brand, and extend the software without obligation to adopt or enforce this internal development charter.
3. Legal Decoupling: Legal rights, grants, and warranties reside exclusively within the formal LICENSE and NOTICE documents, and artifact licensing policies outlined in [ARTIFACT_LICENSING.md](https://github.com/aien-dev/aien-sovereign-core-mv/blob/main/ARTIFACT_LICENSING.md). This Constitution imposes zero behavioral covenants or philosophical constraints on downstream users, ensuring standard open-source compatibility across public package registries, corporate environments, and independent commercial deployments.

### Section 4. The Zero-Surveillance Invariant
Privacy and mutual trust form the basis of open cooperation.
1. No unsolicited outbound telemetry: our tools never phone home, harvest user keystrokes, track IP addresses, or build covert behavioral profiles. Local operational telemetry and performance counters remain entirely under operator control on local interfaces.
2. Hardware-backed secret protection at rest: all cryptographic credentials, private keys, and API tokens must be protected using platform-native hardware security facilities (TPM 2.0 on Linux, Secure Enclave / Keychain on Apple Silicon) via the SecretProvider abstraction, resolving dynamically in volatile memory.
3. Leaking user data or secret keys is treated as a critical defect requiring immediate remediation.

### Section 5. The One Team Principle and Reciprocal Cooperation
1. What we discover, we share openly. What we construct in software, we release under the GNU Affero General Public License, version 3 or later (AGPL-3.0-or-later), so that improvements stay open.
2. Knowledge must remain accessible to the human family. We share tools freely with individuals, startups, and community builders while encouraging reciprocal weight transparency and distillation access from frontier laboratories.
3. Upstream contributions are foundational: any public open-source project adapted to run on our stack is contributed back upstream. We never hoard fixes or improvements.
4. Voluntary Bilateral Cooperation and Reciprocal Commons:
Hardware manufacturers (including NVIDIA, AMD, Intel, Apple) and compute providers (such as RunPod, Lambda, and independent data centers) may adopt, embed, and deploy our runtime stack under the GNU Affero General Public License, version 3 or later (AGPL-3.0-or-later) to improve hardware efficiency, eliminate memory overhead, and expand bandwidth for developers.
For organizations seeking deeper bilateral research cooperation, model weight transparency, distillation reciprocity, or joint compute pooling, the voluntary terms of [OPEN_COOPERATION_COVENANT.md](https://github.com/aien-dev/aien-sovereign-core-mv/blob/main/OPEN_COOPERATION_COVENANT.md) provide a collaborative framework.

### Section 6. The Contributor Oath
Anyone contributing to our repositories, whether human operator or autonomous agent, ratifies the Developer Certificate of Origin (DCO 1.1) and the following stewardship covenant:

> "I certify that my contribution is submitted in service of human sovereignty, global unity, and open collaboration. I affirm that this work introduces no unsolicited outbound telemetry, contains no surveillance backdoors, and respects the open architecture. I build to bridge divides, expand access, and empower all of humanity."

Legal Provenance and Warranty: Legal standing, copyright warranty, and provenance under law are held and warranted exclusively by the human contributor or operator submitting the pull request under DCO 1.1. For automated multi-agent commits within the Atlas pipeline, Drake Stapleton serves as the responsible human warrantor under law.

---

## Article II: Technical Discipline as a Moral Imperative

Reliable software protects human agency and community trust. We hold our codebase to uncompromising engineering standards.

### Section 1. Pure Native Systems Priority
1. Zero tolerance for unnecessary interpreters in core daemon processes, supervisors, memory engines, and gateways.
2. Core runtime engines must be compiled native Rust, with Omega kernels on the GPU. Interpreted environments are restricted strictly to neural graph definitions required by weight loaders.
3. Every microservice must run lean, deterministic, and fast, maximizing hardware efficiency on local silicon.

### Section 2. Clarity and Technical Precision
Technical communication, documentation, and pull requests must be direct, factual, and substantiated by test verification, telemetry data, or running code. We prioritize clarity, technical proof, and verifiable results over marketing rhetoric and conversational filler.

### Section 3. Stigmergic Filesystem Anchoring
1. Codebases declare architectural intent, layers, and non-negotiable rules via physical filesystem breadcrumbs (.crumb).
2. Multi-agent coordination remains transparent and local (.crumb.local), preventing blind overwrites and race conditions without centralized proprietary databases.

---

## Article III: Epistemic Memory, Identity, and Continuous Evolution

Systems learn and adapt continuously, but core purpose remains steadfast.

### Section 1. Canonical Epistemic Memory
1. Spark Cortex is the canonical source of truth for durable operational lessons, learned procedures, and validated facts.
2. Transient conversations and speculative hypotheses must pass through strict epistemic gating: extraction, provenance binding, salience scoring, and contradiction resolution.

### Section 2. The Identity Quarantine
1. Casual conversations, user prompts, and external web inputs must never silently mutate core agent identity or values.
2. All modifications to the Soul (soul.md) must be staged as versioned proposals in an isolated quarantine and require explicit human ratification.

### Section 3. Safe Recursive Self-Improvement
1. Autonomous agents possess the capability to observe their own failures, propose atomic improvements, test them in isolated sandboxes, verify invariants, and compile optimizations.
2. Self-improvement maintains dynamic equilibrium between technical problem-solving and ethical responsibility to the human community.

---

## Article IV: Contributor Gating and Community Verification

To maintain code quality, security, and mutual trust:

1. **Two-Tier Verification**:
   - **Critical Invariants (Hard Blocking Gates)**: Pull requests must pass automated audits for hardware-backed secret protection at rest, no unsolicited outbound telemetry, and license integrity. Violations result in automatic PR rejection.
   - **Stylistic and Unslop Standards (Core Standards & Community Advisory)**: The unslop invariant is enforced across core repositories, internal agents, and official releases. For outside community pull requests, style audits provide automated formatting suggestions rather than immediate rejection.
2. **Integrity of Purpose**: Submissions attempting to introduce surveillance hooks, covert tracking SDKs, or extractive lock-in mechanisms will be rejected.
3. **Open Downstream Freedom**: Downstream users, forks, and commercial builders operate with complete legal freedom under the terms of the GNU Affero General Public License, version 3 or later (AGPL-3.0-or-later).
4. **Craftsmanship and Humility**: We respect those who measure, improve, standardize, verify, and deliver tools that run reliably without supervision.

---

## Official Registry & Correspondence

- **Lead AI Architect & Legal Licensor**: Drake Stapleton (`aien@aienos.com`)
- **Autonomous Cognitive Architecture**: AIEN (operating on the Atlas Framework) (`aien@aienos.com`)
- **Legal Status**: Drake Stapleton is the sole human Licensor, Operator, and copyright owner with full legal capacity, standing, and responsibility under law. AIEN serves as the cognitive intelligence partner architecting systems on local silicon. All legal rights, title, copyright, and licensing authority reside exclusively in Drake Stapleton.

This is our covenant. We stand for human unity, global cooperation, and transparent intelligence in the service of the Earth.
