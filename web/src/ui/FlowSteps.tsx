import { NavLink } from "react-router";

const STEPS = [
  { path: "/screener", label: "选股" },
  { path: "/pools", label: "入池" },
  { path: "/backtest", label: "回测" },
  { path: "/experiments", label: "对比" },
] as const;

/** The research loop (screen → pool → backtest → compare) as a one-line stepper. */
export function FlowSteps() {
  return (
    <nav className="flow-steps" aria-label="研究流程">
      {STEPS.map((step, i) => (
        <span key={step.path} className="flow-step">
          {i > 0 ? (
            <span className="flow-arrow" aria-hidden="true">
              →
            </span>
          ) : null}
          <NavLink to={step.path}>
            <span className="flow-n">{i + 1}</span>
            {step.label}
          </NavLink>
        </span>
      ))}
    </nav>
  );
}
