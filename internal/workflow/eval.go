package workflow

import (
	"fmt"
	"strings"
)

// EvalContext provides variables and parent stage states to condition expressions.
type EvalContext struct {
	ParentStages map[string]*StageState
	Variables    map[string]string
}

// EvaluateCondition evaluates whether a stage should run given its condition, expression, and upstream states.
func EvaluateCondition(cond Condition, expr string, ctx *EvalContext) (bool, error) {
	if ctx == nil {
		ctx = &EvalContext{
			ParentStages: make(map[string]*StageState),
			Variables:    make(map[string]string),
		}
	}

	allSuccess := true
	anyFailure := false
	hasParents := len(ctx.ParentStages) > 0

	for _, parent := range ctx.ParentStages {
		if parent.Status == StageFailed {
			anyFailure = true
			allSuccess = false
		} else if parent.Status != StageCompleted {
			allSuccess = false
		}
	}

	switch cond {
	case ConditionAlways:
		return true, nil

	case ConditionOnFailure:
		if !hasParents {
			return false, nil
		}
		return anyFailure, nil

	case ConditionExpression:
		if strings.TrimSpace(expr) == "" {
			return false, fmt.Errorf("condition expression cannot be empty")
		}
		return evaluateExpression(expr, ctx)

	case ConditionOnSuccess, "":
		if !hasParents {
			return true, nil
		}
		return allSuccess, nil

	default:
		return false, fmt.Errorf("unknown condition type: %s", cond)
	}
}

// evaluateExpression evaluates a simple expression against the context.
// Supported syntax:
//   - "stages.<stage_id>.status == completed"
//   - "stages.<stage_id>.status == failed"
//   - "stages.<stage_id>.status != failed"
//   - "vars.<key> == <value>"
//   - "vars.<key> != <value>"
func evaluateExpression(expr string, ctx *EvalContext) (bool, error) {
	expr = strings.TrimSpace(expr)

	var op string
	if strings.Contains(expr, "==") {
		op = "=="
	} else if strings.Contains(expr, "!=") {
		op = "!="
	} else {
		return false, fmt.Errorf("unsupported operator in expression: %s", expr)
	}

	parts := strings.SplitN(expr, op, 2)
	if len(parts) != 2 {
		return false, fmt.Errorf("invalid expression format: %s", expr)
	}

	left := strings.TrimSpace(parts[0])
	right := strings.Trim(strings.TrimSpace(parts[1]), "\"'")

	var actual string
	if strings.HasPrefix(left, "stages.") {
		stageParts := strings.Split(left, ".")
		if len(stageParts) < 3 {
			return false, fmt.Errorf("invalid stage expression selector: %s", left)
		}
		stageID := stageParts[1]
		attr := stageParts[2]

		parent, ok := ctx.ParentStages[stageID]
		if !ok {
			return false, fmt.Errorf("parent stage %s not found in context", stageID)
		}

		if attr == "status" {
			actual = string(parent.Status)
		} else {
			return false, fmt.Errorf("unsupported stage attribute: %s", attr)
		}
	} else if strings.HasPrefix(left, "vars.") {
		varKey := strings.TrimPrefix(left, "vars.")
		actual = ctx.Variables[varKey]
	} else {
		return false, fmt.Errorf("unrecognized identifier in expression: %s", left)
	}

	if op == "==" {
		return actual == right, nil
	}
	return actual != right, nil
}
