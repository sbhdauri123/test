using Greenhouse.Data.Model.Setup;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Greenhouse.Data.Model.Core
{
    [Serializable]
    public class ExecutionPath
    {
        public IEnumerable<SourceJob> SourceJobSteps { get; set; }
        public int StepIndex { get; set; }

        [JsonConstructor]
        public ExecutionPath()
        {
        }

        public ExecutionPath(IEnumerable<SourceJob> sourceJobSteps)
        {
            SourceJobSteps = sourceJobSteps;
            StepIndex = sourceJobSteps.Min(s => s.StepOrderIndex);
        }

        public SourceJobStep CurrentStep
        {
            get
            {
                return SourceJobSteps.SingleOrDefault(s => s.StepOrderIndex == StepIndex).JobStep;
            }
        }

        public SourceJobStep GotoNextStep()
        {
            var next = SourceJobSteps.OrderBy(o => o.StepOrderIndex).SkipWhile(s => s.StepOrderIndex != StepIndex).Skip(1).FirstOrDefault();
            if (next != null)
            {
                this.StepIndex = next.StepOrderIndex;
            }
            return next == null ? null : next.JobStep;
        }
    }
}
