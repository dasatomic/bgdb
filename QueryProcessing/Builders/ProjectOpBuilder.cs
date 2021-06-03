using MetadataManager;
using Microsoft.FSharp.Core;
using PageManager;
using QueryProcessing.Exceptions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace QueryProcessing
{
    class ProjectOrApplyFuncFunctors
    {
        /// <summary>
        /// Functor that projects rowholder into new one.
        /// Fields that are to be copied are copied.
        /// Fields on which some execution is to be done
        /// (e.g. func call) are kept empty and initialized
        /// with default value.
        /// </summary>
        public Func<RowHolder, RowHolder> ProjectAndExtend { get; }

        /// <summary>
        /// Applies computations on initialized RowHolder
        /// (i.e. one passed from ProjectAndExtend).
        /// </summary>
        public Action<RowHolder> ApplyCompute { get; }

        public MetadataColumn[] ProjectColumnInfo { get; }

        public ProjectOrApplyFuncFunctors(
            Func<RowHolder, RowHolder> projectAndExtend,
            Action<RowHolder> applyCompute,
            MetadataColumn[] projectColumnInfo)
        {
            this.ProjectAndExtend = projectAndExtend;
            this.ApplyCompute = applyCompute;
            this.ProjectColumnInfo = projectColumnInfo;
        }
    }

    class ProjectOpBuilder : IStatementTreeBuilder
    {
        private MetadataColumn[] GetOutputSchema(Sql.columnSelect[] columns, IPhysicalOperator<RowHolder> source)
        {
            MetadataColumn[] result = new MetadataColumn[columns.Length];
            int pos = 0;
            foreach (Sql.columnSelect column in columns)
            {
                if (column.IsAggregate)
                {
                    throw new ArgumentException("Aggregate shouldn't be handled here");
                }

                Sql.columnSelect.ValueOrFunc valueOrFunc = (Sql.columnSelect.ValueOrFunc)column;

                if (valueOrFunc.Item.IsValue)
                {
                    var projection = ((Sql.valueOrFunc.Value)valueOrFunc.Item);
                    if (!projection.Item.IsId)
                    {
                        throw new Exception("Projection on non id is not supported");
                    }

                    string projectionId = ((Sql.value.Id)projection.Item).Item;
                    result[pos] = QueryProcessingAccessors.GetMetadataColumn(projectionId, source.GetOutputColumns());
                }
                else if (valueOrFunc.Item.IsFuncCall)
                {
                    var func = ((Sql.valueOrFunc.FuncCall)valueOrFunc.Item);
                    result[pos] = FuncCallMapper.GetMetadataInfoForOutput(func, source.GetOutputColumns());
                }

                pos++;
            }

            return result;
        }

        private ProjectExtendInfo BuildProjectExtendInfo(Sql.columnSelect[] columns, IPhysicalOperator<RowHolder> source)
        {
            List<int> projectPositions = new List<int>();
            List<ColumnInfo> extendPositions = new List<ColumnInfo>();
            List<ProjectExtendInfo.MappingType> mappingTypes = new List<ProjectExtendInfo.MappingType>();

            foreach (Sql.columnSelect column in columns)
            {
                if (column.IsAggregate)
                {
                    throw new ArgumentException("Aggregate shouldn't be handled here");
                }

                Sql.columnSelect.ValueOrFunc valueOrFunc = (Sql.columnSelect.ValueOrFunc)column;

                if (valueOrFunc.Item.IsValue)
                {
                    // Just projection.
                    mappingTypes.Add(ProjectExtendInfo.MappingType.Projection);
                    var projection = ((Sql.valueOrFunc.Value)valueOrFunc.Item);

                    if (!projection.Item.IsId)
                    {
                        throw new NotImplementedException("Projection on non id is not supported");
                    }

                    string projectionId = ((Sql.value.Id)projection.Item).Item;
                    MetadataColumn mc = QueryProcessingAccessors.GetMetadataColumn(projectionId, source.GetOutputColumns());
                    projectPositions.Add(mc.ColumnId);
                }
                else if (valueOrFunc.Item.IsFuncCall)
                {
                    mappingTypes.Add(ProjectExtendInfo.MappingType.Extension);
                    var func = ((Sql.valueOrFunc.FuncCall)valueOrFunc.Item);
                    ColumnInfo ci = FuncCallMapper.GetMetadataInfoForOutput(func, source.GetOutputColumns()).ColumnType;
                    extendPositions.Add(ci);
                }
                else
                {
                    throw new NotImplementedException("Invalid type in select");
                }
            }

            return new ProjectExtendInfo(mappingTypes.ToArray(), projectPositions.ToArray(), extendPositions.ToArray());
        }

        private Action<RowHolder, RowHolder> ExecuteComputeOnRowHolder(IEnumerable<Sql.columnSelect> selects, MetadataColumn[] sourceColumns)
        {
            int outputPosition = 0;
            List<Action<RowHolder /* input */, RowHolder /* output */>> listOfActions = new List<Action<RowHolder, RowHolder>>();

            foreach (var select in selects)
            {
                Sql.columnSelect.ValueOrFunc valueOrFunc = (Sql.columnSelect.ValueOrFunc)select;
                if (!valueOrFunc.Item.IsFuncCall)
                {
                    outputPosition++;
                    continue;
                }

                var func = ((Sql.valueOrFunc.FuncCall)valueOrFunc.Item);
                listOfActions.Add(FuncCallMapper.BuildRowHolderMapperFunctor(func, outputPosition, sourceColumns));
            }

            // Execute all the actions.
            return ((RowHolder input, RowHolder output) => listOfActions.ForEach(act => act(input, output)));
        }

        public Task<IPhysicalOperator<RowHolder>> BuildStatement(Sql.sqlStatement statement, ITransaction tran, IPhysicalOperator<RowHolder> source, InputStringNormalizer inputStringNormalizer)
        {
            Sql.columnSelect[] columns = new Sql.columnSelect[0];
            bool isStar = false;

            if (statement.GroupBy.Any())
            {
                // no job for me, this is group by.
                return Task.FromResult(source);
            }

            if (!statement.Columns.IsStar)
            {
                columns = (((Sql.selectType.ColumnList)statement.Columns).Item).ToArray();

                if (columns.Any(c => c.IsAggregate == true))
                {
                    // No job for me, this is aggregation.
                    return Task.FromResult(source);
                }
            }
            else
            {
                isStar = true;
            }

            int? topRows = null;
            if (FSharpOption<int>.get_IsSome(statement.Top))
            {
                topRows = statement.Top.Value;

                if (topRows < 1)
                {
                    throw new InvalidTopCountException();
                }
            }

            if (isStar)
            {
                // no need for project, just return everything.
                IPhysicalOperator<RowHolder> projectOp = new PhyOpProject(source, topRows);
                return Task.FromResult(projectOp);
            }
            else
            {
                // Project Op.
                List<MetadataColumn> columnMapping = new List<MetadataColumn>();

                ProjectExtendInfo extendInfo = this.BuildProjectExtendInfo(columns, source);
                Func<RowHolder, RowHolder> projector = (rowHolder) => rowHolder.ProjectAndExtend(extendInfo);
                Action<RowHolder, RowHolder> computes = ExecuteComputeOnRowHolder(columns, source.GetOutputColumns());
                MetadataColumn[] outputSchema = this.GetOutputSchema(columns, source);

                PhyOpProjectComputeFunctors functors = new PhyOpProjectComputeFunctors(projector, computes);

                IPhysicalOperator<RowHolder> projectOp = new PhyOpProject(source, functors, outputSchema, topRows);
                return Task.FromResult(projectOp);
            }
        }
    }
}
