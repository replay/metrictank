package expr

import (
	"errors"
	"fmt"
	"io"

	"github.com/grafana/metrictank/api/models"
	"github.com/grafana/metrictank/consolidation"
)

// Req represents a request for one/more series
type Req struct {
	Query string // whatever was parsed as the query out of a graphite target. e.g. target=sum(foo.{b,a}r.*) -> foo.{b,a}r.* -> this will go straight to index lookup
	From  uint32
	To    uint32
	Cons  consolidation.Consolidator // can be 0 to mean undefined
}

// NewReq creates a new Req. pass cons=0 to leave consolidator undefined,
// leaving up to the caller (in graphite's case, it would cause a lookup into storage-aggregation.conf)
func NewReq(query string, from, to uint32, cons consolidation.Consolidator) Req {
	return Req{
		Query: query,
		From:  from,
		To:    to,
		Cons:  cons,
	}
}

type Plan struct {
	Reqs          []Req          // data that needs to be fetched before functions can be executed
	funcs         []GraphiteFunc // top-level funcs to execute, the head of each tree for each target
	exprs         []*expr
	MaxDataPoints uint32
	From          uint32                  // global request scoped from
	To            uint32                  // global request scoped to
	data          map[Req][]models.Series // input data to work with. set via Run(), as well as
	// new data generated by processing funcs. useful for two reasons:
	// 1) reuse partial calculations e.g. queries like target=movingAvg(sum(foo), 10)&target=sum(foo) (TODO)
	// 2) central place to return data back to pool when we're done.
}

func (p Plan) Dump(w io.Writer) {
	fmt.Fprintf(w, "Plan:\n")
	fmt.Fprintf(w, "* Exprs:\n")
	for _, e := range p.exprs {
		fmt.Fprintln(w, e.Print(2))
	}
	fmt.Fprintf(w, "* Reqs:\n")
	for _, r := range p.Reqs {
		fmt.Fprintln(w, "   ", r)
	}
	fmt.Fprintf(w, "MaxDataPoints: %d\n", p.MaxDataPoints)
	fmt.Fprintf(w, "From: %d\n", p.From)
	fmt.Fprintf(w, "To: %d\n", p.To)
}

// Plan validates the expressions and comes up with the initial (potentially non-optimal) execution plan
// which is just a list of requests and the expressions.
// traverse tree and as we go down:
// * make sure function exists
// * validation of arguments
// * allow functions to modify the Context (change data range or consolidation)
// * future version: allow functions to mark safe to pre-aggregate using consolidateBy or not
func NewPlan(exprs []*expr, from, to, mdp uint32, stable bool, reqs []Req) (Plan, error) {
	var err error
	var funcs []GraphiteFunc
	for _, e := range exprs {
		var fn GraphiteFunc
		context := Context{
			from: from,
			to:   to,
		}
		fn, reqs, err = newplan(e, context, stable, reqs)
		if err != nil {
			return Plan{}, err
		}
		funcs = append(funcs, fn)
	}
	return Plan{
		Reqs:          reqs,
		exprs:         exprs,
		funcs:         funcs,
		MaxDataPoints: mdp,
		From:          from,
		To:            to,
	}, nil
}

// newplan adds requests as needed for the given expr, resolving function calls as needed
func newplan(e *expr, context Context, stable bool, reqs []Req) (GraphiteFunc, []Req, error) {
	if e.etype != etFunc && e.etype != etName {
		return nil, nil, errors.New("request must be a function call or metric pattern")
	}
	if e.etype == etName {
		req := NewReq(e.str, context.from, context.to, context.consol)
		reqs = append(reqs, req)
		return NewGet(req), reqs, nil
	} else if e.etype == etFunc && e.str == "seriesByTag" {
		// `seriesByTag` function requires resolving expressions to series
		// (similar to path expressions handled above). Since we need the
		// arguments of seriesByTag to do the resolution, we store the function
		// string back into the Query member of a new request to be parsed later.
		// TODO - find a way to prevent this parse/encode/parse/encode loop
		expressionStr := "seriesByTag(" + e.argsStr + ")"
		req := NewReq(expressionStr, context.from, context.to, context.consol)
		reqs = append(reqs, req)
		return NewGet(req), reqs, nil
	}
	// here e.type is guaranteed to be etFunc
	fdef, ok := funcs[e.str]
	if !ok {
		return nil, nil, ErrUnknownFunction(e.str)
	}
	if stable && !fdef.stable {
		return nil, nil, ErrUnknownFunction(e.str)
	}

	fn := fdef.constr()
	reqs, err := newplanFunc(e, fn, context, stable, reqs)
	return fn, reqs, err
}

// newplanFunc adds requests as needed for the given expr, and validates the function input
// provided you already know the expression is a function call to the given function
func newplanFunc(e *expr, fn GraphiteFunc, context Context, stable bool, reqs []Req) ([]Req, error) {
	// first comes the interesting task of validating the arguments as specified by the function,
	// against the arguments that were parsed.

	argsExp, _ := fn.Signature()
	var err error

	// note:
	// * signature may have seriesLists in it, which means one or more args of type seriesList
	//   so it's legal to have more e.args than signature args in that case.
	// * we can't do extensive, accurate validation of the type here because what the output from a function we depend on
	//   might be dynamically typed. e.g. movingAvg returns 1..N series depending on how many it got as input

	// first validate the mandatory args
	pos := 0    // e.args[pos]     : next given arg to process
	cutoff := 0 // argsExp[cutoff] : will be first optional arg (if any)
	var argExp Arg
	for cutoff, argExp = range argsExp {
		if argExp.Optional() {
			break
		}
		if len(e.args) <= pos {
			return nil, ErrMissingArg
		}
		pos, err = e.consumeBasicArg(pos, argExp)
		if err != nil {
			return nil, err
		}
	}
	if !argExp.Optional() {
		cutoff++
	}

	// we stopped iterating the mandatory args.
	// any remaining args should be due to optional args otherwise there's too many
	// we also track here which keywords can also be used for the given optional args
	// so that those args should not be specified via their keys anymore.

	seenKwargs := make(map[string]struct{})
	for _, argOpt := range argsExp[cutoff:] {
		if len(e.args) <= pos {
			break // no more args specified. we're done.
		}
		pos, err = e.consumeBasicArg(pos, argOpt)
		if err != nil {
			return nil, err
		}
		seenKwargs[argOpt.Key()] = struct{}{}
	}
	if len(e.args) > pos {
		return nil, ErrTooManyArg
	}

	// for any provided keyword args, verify that they are what the function stipulated
	// and that they have not already been specified via their position
	for key := range e.namedArgs {
		_, ok := seenKwargs[key]
		if ok {
			return nil, ErrKwargSpecifiedTwice{key}
		}
		err = e.consumeKwarg(key, argsExp[cutoff:])
		if err != nil {
			return nil, err
		}
		seenKwargs[key] = struct{}{}
	}

	// functions now have their non-series input args set,
	// so they should now be able to specify any context alterations
	context = fn.Context(context)
	// now that we know the needed context for the data coming into
	// this function, we can set up the input arguments for the function
	// that are series
	pos = 0
	for _, argExp = range argsExp {
		if pos >= len(e.args) {
			break // no more args specified. we're done.
		}
		switch argExp.(type) {
		case ArgSeries, ArgSeriesList, ArgSeriesLists, ArgIn:
			pos, reqs, err = e.consumeSeriesArg(pos, argExp, context, stable, reqs)
			if err != nil {
				return nil, err
			}
		default:
			pos++
		}
	}
	return reqs, err
}

// Run invokes all processing as specified in the plan (expressions, from/to) with the input as input
func (p Plan) Run(input map[Req][]models.Series) ([]models.Series, error) {
	var out []models.Series
	p.data = input
	for _, fn := range p.funcs {
		series, err := fn.Exec(p.data)
		if err != nil {
			return nil, err
		}
		out = append(out, series...)
	}
	for i, o := range out {
		if p.MaxDataPoints != 0 && len(o.Datapoints) > int(p.MaxDataPoints) {
			// series may have been created by a function that didn't know which consolidation function to default to.
			// in the future maybe we can do more clever things here. e.g. perSecond maybe consolidate by max.
			if o.Consolidator == 0 {
				o.Consolidator = consolidation.Avg
			}
			out[i].Datapoints, out[i].Interval = consolidation.ConsolidateNudged(o.Datapoints, o.Interval, p.MaxDataPoints, o.Consolidator)
		}
	}
	return out, nil
}

// Clean returns all buffers (all input data + generated series along the way)
// back to the pool.
func (p Plan) Clean() {
	for _, series := range p.data {
		for _, serie := range series {
			pointSlicePool.Put(serie.Datapoints[:0])
		}
	}
}
