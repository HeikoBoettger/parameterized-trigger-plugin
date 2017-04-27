package hudson.plugins.parameterizedtrigger;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import hudson.EnvVars;
import hudson.Extension;
import hudson.Launcher;
import hudson.model.AbstractBuild;
import hudson.model.AbstractProject;
import hudson.model.Action;
import hudson.model.AutoCompletionCandidates;
import hudson.model.BuildListener;
import hudson.model.Cause;
import hudson.model.CauseAction;
import hudson.model.Cause.UpstreamCause;
import hudson.model.Cause.UserIdCause;
import hudson.model.Queue.QueueAction;
import hudson.model.Describable;
import hudson.model.Descriptor;
import hudson.model.Executor;
import hudson.model.Hudson;
import hudson.model.Item;
import hudson.model.ItemGroup;
import hudson.model.Items;
import hudson.model.Job;
import hudson.model.ParametersAction;
import hudson.model.Result;
import hudson.model.Run;
import hudson.model.TaskListener;
import hudson.model.queue.QueueTaskFuture;
import hudson.model.queue.Tasks;
import hudson.plugins.parameterizedtrigger.AbstractBuildParameters.DontTriggerException;
import hudson.plugins.promoted_builds.Promotion;
import hudson.security.ACL;
import hudson.tasks.Messages;
import hudson.Util;
import hudson.util.FormValidation;
import hudson.util.VersionNumber;

import jenkins.model.Jenkins;
import jenkins.model.ParameterizedJobMixIn;
import jenkins.security.QueueItemAuthenticatorConfiguration;
import org.acegisecurity.Authentication;
import org.apache.commons.lang.StringUtils;
import org.kohsuke.accmod.Restricted;
import org.kohsuke.accmod.restrictions.DoNotUse;
import org.kohsuke.stapler.AncestorInPath;
import org.kohsuke.stapler.DataBoundConstructor;
import org.kohsuke.stapler.QueryParameter;

import javax.annotation.CheckForNull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.annotation.Nonnull;
import org.acegisecurity.AccessDeniedException;

public class BuildTriggerConfig implements Describable<BuildTriggerConfig> {

	private final List<AbstractBuildParameters> configs;
    private final List<AbstractBuildParameterFactory> configFactories;

	private String projects;
	private final ResultCondition condition;
	private boolean triggerWithNoParameters;
        private boolean triggerFromChildProjects;

    public BuildTriggerConfig(String projects, ResultCondition condition, boolean triggerWithNoParameters, 
            List<AbstractBuildParameterFactory> configFactories, List<AbstractBuildParameters> configs, boolean triggerFromChildProjects) {
        this.projects = projects;
        this.condition = condition;
        this.triggerWithNoParameters = triggerWithNoParameters;
        this.configFactories = configFactories;
        this.configs = Util.fixNull(configs);
        this.triggerFromChildProjects = triggerFromChildProjects;
    }

    @Deprecated
    public BuildTriggerConfig(String projects, ResultCondition condition,
            boolean triggerWithNoParameters, List<AbstractBuildParameterFactory> configFactories, List<AbstractBuildParameters> configs) {
        this(projects, condition, triggerWithNoParameters, configFactories, configs, false);
    }

    @DataBoundConstructor
    public BuildTriggerConfig(String projects, ResultCondition condition,
            boolean triggerWithNoParameters, List<AbstractBuildParameters> configs, boolean triggerFromChildProjects) {
        this(projects, condition, triggerWithNoParameters, null, configs, triggerFromChildProjects);
    }
    
    public BuildTriggerConfig(String projects, ResultCondition condition,
            boolean triggerWithNoParameters, List<AbstractBuildParameters> configs) {
        this(projects, condition, triggerWithNoParameters, null, configs);
    }

	public BuildTriggerConfig(String projects, ResultCondition condition,
			AbstractBuildParameters... configs) {
		this(projects, condition, false, null, Arrays.asList(configs));
	}

	public BuildTriggerConfig(String projects, ResultCondition condition,
            List<AbstractBuildParameterFactory> configFactories,
			AbstractBuildParameters... configs) {
		this(projects, condition, false, configFactories, Arrays.asList(configs));
	}

	public List<AbstractBuildParameters> getConfigs() {
		return configs;
	}

    public List<AbstractBuildParameterFactory> getConfigFactories() {
        return configFactories;
    }

    public String getProjects() {
		return projects;
	}

    public String getProjects(EnvVars env) {
        return (env != null ? env.expand(projects) : projects);
    }

	public ResultCondition getCondition() {
		return condition;
	}

	public boolean getTriggerWithNoParameters() {
        return triggerWithNoParameters;
    }
        
    public boolean isTriggerFromChildProjects(){
        return triggerFromChildProjects;
    }

    /**
     * @deprecated
     *      Use {@link #getJobs(ItemGroup, EnvVars)}
     */
    public List<AbstractProject> getProjectList(EnvVars env) {
        return getProjectList(null, env);
    }

    /**
     * Deprecated: get all projects that are AbstractProject instances
     * @param env Environment variables from which to expand project names; Might be {@code null}.
     * @param context
     *      The container with which to resolve relative project names.
     * @deprecated
     *      Use {@link #getJobs(ItemGroup, EnvVars)}
     */
    @Deprecated
	public List<AbstractProject> getProjectList(ItemGroup context, EnvVars env) {
        return Util.filter(getJobs(context, env), AbstractProject.class);
	}

    /**
     * Get list of all projects, including workflow job types
     * @param env Environment variables from which to expand project names; Might be {@code null}.
     * @param context
     *      The container with which to resolve relative project names.
     *      If the user has no {@link Item#READ} permission, the job won't be added to the list.
     */
    public List<Job> getJobs(ItemGroup context, EnvVars env) {
        List<Job> projectList = new ArrayList<Job>();
        projectList.addAll(readableItemsFromNameList(context, getProjects(env), Job.class));
        return projectList;
    }

    /**
     * Provides a SubProjectData object containing four set, each containing projects to be displayed on the project
     * view under 'Subprojects' section.<br>
     * <ul>
     * <li>The first set contains fixed (statically) configured project to be trigger.</li>
     * <li>The second set contains dynamically configured project, resolved by back tracking builds environment variables.</li>
     * <li>The third set contains other recently triggered project found during back tracking builds</li>
     * <li>The fourth set contains dynamically configured project that couldn't be resolved or project that doesn't exists.</li>
     * </ul>
     *
     * @param context   The container with which to resolve relative project names.
     * @return A data object containing sets with projects
     */
    public SubProjectData getProjectInfo(AbstractProject context) {

        SubProjectData subProjectData = new SubProjectData();

        iterateBuilds(context, projects, subProjectData);

        // We don't want to show a project twice
        subProjectData.getTriggered().removeAll(subProjectData.getDynamic());
        subProjectData.getTriggered().removeAll(subProjectData.getFixed());

        return subProjectData;
    }

    /**
     * Resolves fixed (static) project and iterating old builds to resolve dynamic and collecting triggered
     * projects.<br>
     * <br>
     * If fixed project and/or resolved projects exists they are returned in fixed or dynamic in subProjectData.
     * If old builds exists it tries to resolve projects by back tracking the last five builds and as a last resource
     * the last successful build.<br>
     * <br>
     * During the back tracking process all actually trigger projects from those builds are also collected and stored
     * in triggered in subProjectData.<br>
     * <br>
     *
     * @param context           The container with which to resolve relative project names.
     * @param projects          String containing the defined projects to build
     * @param subProjectData    Data object containing sets storing projects
     */
    private static void iterateBuilds(AbstractProject context, String projects, SubProjectData subProjectData) {

        StringTokenizer stringTokenizer = new StringTokenizer(projects, ",");
        while (stringTokenizer.hasMoreTokens()) {
            subProjectData.getUnresolved().add(stringTokenizer.nextToken().trim());
        }

        // Nbr of builds to back track
        final int BACK_TRACK = 5;

        if (!subProjectData.getUnresolved().isEmpty()) {

            AbstractBuild currentBuild = (AbstractBuild)context.getLastBuild();

            // If we don't have any build there's no point to trying to resolved dynamic projects
            if (currentBuild == null) {
                // But we can still get statically defined project
                subProjectData.getFixed().addAll(readableItemsFromNameList(context.getParent(), projects, AbstractProject.class));
                
                // Remove them from unsolved
                for (AbstractProject staticProject : subProjectData.getFixed()) {
                    subProjectData.getUnresolved().remove(staticProject.getFullName());
                }
                return;
            }

            // check the last build
            resolveProject(currentBuild, subProjectData);
            currentBuild = (AbstractBuild)currentBuild.getPreviousBuild();

            int backTrackCount = 0;
            // as long we have more builds to examine we continue,
            while (currentBuild != null && backTrackCount < BACK_TRACK) {
                resolveProject(currentBuild, subProjectData);
                currentBuild = (AbstractBuild)currentBuild.getPreviousBuild();
                backTrackCount++;
            }

            // If oldBuild is null then we have already examined LastSuccessfulBuild as well.
            if (currentBuild != null && context.getLastSuccessfulBuild() != null) {
                resolveProject((AbstractBuild)context.getLastSuccessfulBuild(), subProjectData);
            }
        }
    }
    
    /**
     * Retrieves readable items from the list.
     * @param <T> Type of the item
     * @param context Current item
     * @param list String list of items
     * @param type Type of items to be retrieved
     * @return List of readable items, others will be skipped if {@link AccessDeniedException} happens
     */
    private static <T extends Item> List<T> readableItemsFromNameList(
            ItemGroup context, @Nonnull String list, @Nonnull Class<T> type) {
        Jenkins hudson = Jenkins.getInstance();

        List<T> r = new ArrayList<T>();
        StringTokenizer tokens = new StringTokenizer(list,",");
        while(tokens.hasMoreTokens()) {
            String fullName = tokens.nextToken().trim();
            T item = null;
            try {
                item = hudson.getItem(fullName, context, type);
            } catch (AccessDeniedException ex) {
                // Ignore, item won't be added to the resulting list
            }
            if(item!=null)
                r.add(item);
        }
        return r;
    }

    /**
     * Retrieves the environment variable from a build and tries to resolves the remaining unresolved projects. If
     * resolved it ends up either in the dynamic or fixed in subProjectData. It also collect all actually triggered
     * project and store them in triggered in subProjectData.
     *
     * @param build             The build to retrieve environment variables from and collect triggered projects
     * @param subProjectData    Data object containing sets storing projects
     */
    private static void resolveProject(AbstractBuild build, SubProjectData subProjectData) {

        Iterator<String> unsolvedProjectIterator = subProjectData.getUnresolved().iterator();

        while (unsolvedProjectIterator.hasNext()) {

            String unresolvedProjectName = unsolvedProjectIterator.next();
            Set<AbstractProject> destinationSet = subProjectData.getFixed();

            // expand variables if applicable
            if (unresolvedProjectName.contains("$")) {

                EnvVars env = null;
                try {
                    env = build != null ? build.getEnvironment() : null;
                } catch (IOException e) {
                } catch (InterruptedException e) {
                }

                unresolvedProjectName = env != null ? env.expand(unresolvedProjectName) : unresolvedProjectName;
                destinationSet = subProjectData.getDynamic();
            }

            final Jenkins jenkins = Jenkins.getInstance();
            AbstractProject resolvedProject = null;
            try {
                resolvedProject = jenkins == null ? null :
                        jenkins.getItem(unresolvedProjectName, build.getProject().getParent(), AbstractProject.class);
            } catch (AccessDeniedException ex) {
                // Permission check failure (DISCOVER w/o READ) => we leave the job unresolved
            }
            if (resolvedProject != null) {
                destinationSet.add(resolvedProject);
                unsolvedProjectIterator.remove();
            }
        }

        if (build != null && build.getAction(BuildInfoExporterAction.class) != null) {
            String triggeredProjects = build.getAction(BuildInfoExporterAction.class).getProjectListString(",");
            subProjectData.getTriggered().addAll(readableItemsFromNameList(build.getParent().getParent(), triggeredProjects, AbstractProject.class));
        }
    }


    List<Action> getBaseActions(AbstractBuild<?,?> build, TaskListener listener)
            throws IOException, InterruptedException, DontTriggerException {
        return getBaseActions(configs, build, listener);
    }

    List<Action> getBaseActions(Collection<AbstractBuildParameters> configs, AbstractBuild<?,?> build, TaskListener listener)
            throws IOException, InterruptedException, DontTriggerException {
		List<Action> actions = new ArrayList<Action>();
		ParametersAction params = null;
		for (AbstractBuildParameters config : configs) {
			Action a = config.getAction(build, listener);
			if (a instanceof ParametersAction) {
				params = params == null ? (ParametersAction)a
					: ParameterizedTriggerUtils.mergeParameters(params, (ParametersAction)a);
			} else if (a != null) {
				actions.add(a);
			}
		}
		if (params != null) actions.add(params);
		return actions;
	}

    List<Action> getBuildActions(List<Action> baseActions, Job<?,?> project) {
            List<Action> actions = new ArrayList<Action>(baseActions);

            ProjectSpecificParametersActionFactory transformer = new ProjectSpecificParametersActionFactory(
                    new ProjectSpecificParameterValuesActionTransform(),
                    new DefaultParameterValuesActionsTransform()
            );

            return transformer.getProjectSpecificBuildActions(actions, project);
    }

    /**
     * Note that with Hudson 1.341, trigger should be using
	 * {@link BuildTrigger#buildDependencyGraph(AbstractProject, hudson.model.DependencyGraph)}.
	 */
	public List<QueueTaskFuture<? extends AbstractBuild>> perform(AbstractBuild<?, ?> build, Launcher launcher,
			BuildListener listener) throws InterruptedException, IOException {
        EnvVars env = build.getEnvironment(listener);
        env.overrideAll(build.getBuildVariables());

        try {
			if (condition.isMet(build.getResult())) {
                List<QueueTaskFuture<? extends AbstractBuild>> futures = new ArrayList<QueueTaskFuture<? extends AbstractBuild>>();

                for (List<AbstractBuildParameters> addConfigs : getDynamicBuildParameters(build, listener)) {
                    List<Action> actions = getBaseActions(
                            ImmutableList.<AbstractBuildParameters>builder().addAll(configs).addAll(addConfigs).build(),
                            build, listener);
                    for (Job project : getJobs(build.getRootBuild().getProject().getParent(), env)) {
                        List<Action> list = getBuildActions(actions, project);

                        futures.add(schedule(build, project, list));
                    }
                }

                return futures;
			}
		} catch (DontTriggerException e) {
			// don't trigger on this configuration
		}
        return Collections.emptyList();
	}


    /**
    *  @deprecated
    *      Use {@link #perform3(AbstractBuild, Launcher, BuildListener)}
    */
    @Deprecated
    public ListMultimap<AbstractProject, QueueTaskFuture<? extends AbstractBuild>> perform2(AbstractBuild<?, ?> build, Launcher launcher, BuildListener listener) throws InterruptedException, IOException {
        ListMultimap<Job, QueueTaskFuture<? extends AbstractBuild>> initialResult = perform3(build, launcher, listener);
        ListMultimap<AbstractProject, QueueTaskFuture<? extends AbstractBuild>> output = ArrayListMultimap.create();

        for (Map.Entry<Job, QueueTaskFuture<? extends AbstractBuild>> entry : initialResult.entries()) {
            if (entry.getKey() instanceof AbstractProject) {
                // Due to type erasure we can't check if the Future<Run> is a Future<AbstractBuild>
                // Plugins extending the method and dependent on the perform2 method will break if we trigger on a WorkflowJob
                output.put((AbstractProject)entry.getKey(), entry.getValue());
            }
        }

        return output;
    }

    //Replaces perform2 with more general form
    public ListMultimap<Job, QueueTaskFuture<? extends AbstractBuild>> perform3(AbstractBuild<?, ?> build, Launcher launcher, BuildListener listener) throws InterruptedException, IOException {
        EnvVars env = build.getEnvironment(listener);
        env.overrideAll(build.getBuildVariables());

        try {
            if (getCondition().isMet(build.getResult())) {
                ListMultimap<Job, QueueTaskFuture<? extends AbstractBuild>> futures = ArrayListMultimap.create();

                for (List<AbstractBuildParameters> addConfigs : getDynamicBuildParameters(build, listener)) {
                    List<Action> actions = getBaseActions(ImmutableList.<AbstractBuildParameters>builder().addAll(configs).addAll(addConfigs).build(), build, listener);
                    for (Job project : getJobs(build.getRootBuild().getProject().getParent(), env)) {
                        List<Action> list = getBuildActions(actions, project);

                        futures.put(project, schedule(build, project, list));
                    }
                }
                return futures;
            }
        } catch (DontTriggerException e) {
            // don't trigger on this configuration
        }
        return ArrayListMultimap.create();
    }

    /**
     * @return
     *      Inner list represents a set of build parameters used together for one invocation of a project,
     *      and outer list represents multiple invocations of the same project.
     */
    private List<List<AbstractBuildParameters>> getDynamicBuildParameters(AbstractBuild<?,?> build, BuildListener listener) throws DontTriggerException, IOException, InterruptedException {
        if (configFactories == null || configFactories.isEmpty()) {
            return ImmutableList.<List<AbstractBuildParameters>>of(ImmutableList.<AbstractBuildParameters>of());
        } else {
            // this code is building the combinations of all AbstractBuildParameters reported from all factories
            List<List<AbstractBuildParameters>> dynamicBuildParameters = Lists.newArrayList();
            dynamicBuildParameters.add(Collections.<AbstractBuildParameters>emptyList());
            for (AbstractBuildParameterFactory configFactory : configFactories) {
                List<List<AbstractBuildParameters>> newDynParameters = Lists.newArrayList();
                List<AbstractBuildParameters> factoryParameters = configFactory.getParameters(build, listener);
                // if factory returns 0 parameters we need to skip assigning newDynParameters to dynamicBuildParameters as we would add invalid list
                if(factoryParameters.size() > 0) {
                    for (AbstractBuildParameters config : factoryParameters) {
                        for (List<AbstractBuildParameters> dynamicBuildParameter : dynamicBuildParameters) {
                            newDynParameters.add(
                                    ImmutableList.<AbstractBuildParameters>builder()
                                            .addAll(dynamicBuildParameter)
                                            .add(config)
                                            .build());
                        }
                    }
                    dynamicBuildParameters = newDynParameters;
                }
            }
            return dynamicBuildParameters;
        }
    }

    /**
     * Create UpstreamCause that triggers a downstream build.
     *
     * If the upstream build is a promotion, return the UpstreamCause
     * as triggered by the target of the promotion.
     *
     * @param build an upstream build
     * @return UpstreamCause
     */
    protected Cause createUpstreamCause(Run<?, ?> build) {
        if(Jenkins.getInstance().getPlugin("promoted-builds") != null) {
            // Test only when promoted-builds is installed.
            if(build instanceof Promotion) {
                Promotion promotion = (Promotion)build;

                // This cannot be done for PromotionCause#PromotionCause is in a package scope.
                // return new PromotionCause(build, promotion.getTarget());

                return new UpstreamCause((Run<?,?>)promotion.getTarget());
            }
        }
        return new UpstreamCause(build);
    }

    @CheckForNull
    protected QueueTaskFuture<? extends AbstractBuild> schedule(AbstractBuild<?, ?> build, final Job<?, ? extends AbstractBuild> project, int quietPeriod, List<Action> list) throws InterruptedException, IOException {
        // TODO Once it's in core (since 1.621) and LTS is out, switch to use new ParameterizedJobMixIn convenience method
        // From https://github.com/jenkinsci/jenkins/pull/1771
        Cause cause = createUpstreamCause(build);
        List<Action> queueActions = new ArrayList<Action>(list);
        if (cause != null) {
            queueActions.add(new CauseAction(cause));
        }

        // Includes both traditional projects via AbstractProject and Workflow Job
        if (project instanceof ParameterizedJobMixIn.ParameterizedJob) {
            final ParameterizedJobMixIn<?, ? extends AbstractBuild> parameterizedJobMixIn = new ParameterizedJobMixIn() {
                @Override
                protected Job<?, ? extends AbstractBuild> asJob() {
                    return project;
                }
            };
            
            QueueTaskFuture<? extends AbstractBuild> existingBuild = connectToExistingBuild(build, project, queueActions);
            if (existingBuild != null)
            {
                return existingBuild;
            }
            return parameterizedJobMixIn.scheduleBuild2(quietPeriod, queueActions.toArray(new Action[queueActions.size()]));
        }

        // Trigger is not compatible with un-parameterized jobs
        return null;
    }

    private QueueTaskFuture<? extends AbstractBuild> connectToExistingBuild(AbstractBuild<?, ?> currentBuild, Job<?, ? extends AbstractBuild> project, List<Action> actions) {
        for (final AbstractBuild build: project.getBuilds())
        {
            boolean shouldScheduleItem = false;
            for (QueueAction action: build.getActions(QueueAction.class)) {
                shouldScheduleItem |= action.shouldSchedule(actions);
            }
            for (QueueAction action: Util.filter(actions,QueueAction.class)) {
                shouldScheduleItem |= action.shouldSchedule((new ArrayList<Action>(build.getAllActions())));
            }
            if(!shouldScheduleItem) {
                //found a matching build
                return new QueueTaskFuture<AbstractBuild>() {
                    
                    @Override
                    public boolean cancel(boolean arg0) {
                        if (build.isBuilding())
                        {
                            Executor executor = build.getExecutor();
                            if (executor != null) executor.interrupt();
                            return true;
                        }
                        return false;
                    }
                
                    @Override
                    public AbstractBuild get() throws InterruptedException, ExecutionException {
                        return build;
                    }
                
                    @Override
                    public AbstractBuild get(long arg0, TimeUnit arg1)
                            throws InterruptedException, ExecutionException, TimeoutException {
                        return build;
                    }
                
                    @Override
                    public boolean isCancelled() {
                        return Result.ABORTED.equals(build.getResult());
                    }
                
                    @Override
                    public boolean isDone() {
                        return !build.isBuilding();
                    }
                
                    @Override
                    public Future<AbstractBuild> getStartCondition() {
                        return this; //there is no difference
                    }
                
                    @Override
                    public AbstractBuild waitForStart() throws InterruptedException, ExecutionException {
                        return build;
                    }
                };
            }
        }
        return null;
    }

    protected QueueTaskFuture<? extends AbstractBuild> schedule(AbstractBuild<?, ?> build, Job project, List<Action> list) throws InterruptedException, IOException {
        if (project instanceof ParameterizedJobMixIn.ParameterizedJob) {
            return schedule(build, project, ((ParameterizedJobMixIn.ParameterizedJob) project).getQuietPeriod(), list);
        } else {
            return schedule(build, project, 0, list);
        }
    }

    /**
     * A backport of {@link Items#computeRelativeNamesAfterRenaming(String, String, String, ItemGroup)} in Jenkins 1.530.
     *
     * computeRelativeNamesAfterRenaming contains a bug in Jenkins < 1.530.
     * Replace this to {@link Items#computeRelativeNamesAfterRenaming(String, String, String, ItemGroup)}
     * when updated the target version to >= 1.530.
     *
     * @param oldFullName
     * @param newFullName
     * @param relativeNames
     * @param context
     * @return
     */
    private static String computeRelativeNamesAfterRenaming(String oldFullName, String newFullName, String relativeNames, ItemGroup<?> context) {
        if(!Jenkins.getVersion().isOlderThan(new VersionNumber("1.530"))) {
            return Items.computeRelativeNamesAfterRenaming(oldFullName, newFullName, relativeNames, context);
        }
        StringTokenizer tokens = new StringTokenizer(relativeNames,",");
        List<String> newValue = new ArrayList<String>();
        while(tokens.hasMoreTokens()) {
            String relativeName = tokens.nextToken().trim();
            String canonicalName = Items.getCanonicalName(context, relativeName);
            if (canonicalName.equals(oldFullName) || canonicalName.startsWith(oldFullName + "/")) {
                String newCanonicalName = newFullName + canonicalName.substring(oldFullName.length());
                // relative name points to the renamed item, let's compute the new relative name
                newValue.add( computeRelativeNameAfterRenaming(canonicalName, newCanonicalName, relativeName) );
            } else {
                newValue.add(relativeName);
            }
        }
        return StringUtils.join(newValue, ",");
    }

    private static String computeRelativeNameAfterRenaming(String oldFullName, String newFullName, String relativeName) {

        String[] a = oldFullName.split("/");
        String[] n = newFullName.split("/");
        assert a.length == n.length;
        String[] r = relativeName.split("/");

        int j = a.length-1;
        for(int i=r.length-1;i>=0;i--) {
            String part = r[i];
            if (part.equals("") && i==0) {
                continue;
            }
            if (part.equals(".")) {
                continue;
            }
            if (part.equals("..")) {
                j--;
                continue;
            }
            if (part.equals(a[j])) {
                r[i] = n[j];
                j--;
                continue;
            }
        }
        return StringUtils.join(r, '/');
    }

    public boolean onJobRenamed(ItemGroup context, String oldName, String newName) {
        String newProjects = computeRelativeNamesAfterRenaming(oldName, newName, projects, context);
    	boolean changed = !projects.equals(newProjects);
        projects = newProjects;
    	return changed;
    }

    public boolean onDeleted(ItemGroup context, String oldName) {
        List<String> newNames = new ArrayList<String>();
        StringTokenizer tokens = new StringTokenizer(projects,",");
        List<String> newValue = new ArrayList<String>();
        while (tokens.hasMoreTokens()) {
            String relativeName = tokens.nextToken().trim();
            String fullName = Items.getCanonicalName(context, relativeName);
            if (!fullName.equals(oldName)) newNames.add(relativeName);
        }
        String newProjects = StringUtils.join(newNames, ",");
        boolean changed = !projects.equals(newProjects);
        projects = newProjects;
        return changed;
    }

    public Descriptor<BuildTriggerConfig> getDescriptor() {
        return Hudson.getInstance().getDescriptorOrDie(getClass());
    }

    @Override
	public String toString() {
		return getClass().getName()+" [projects=" + projects + ", condition="
				+ condition + ", configs=" + configs + "]";
	}

    @Extension
    public static class DescriptorImpl extends Descriptor<BuildTriggerConfig> {
        @Override
        public String getDisplayName() {
            return ""; // unused
        }

        public List<Descriptor<AbstractBuildParameters>> getBuilderConfigDescriptors() {
            return Hudson.getInstance().<AbstractBuildParameters,
              Descriptor<AbstractBuildParameters>>getDescriptorList(AbstractBuildParameters.class);
        }

        public List<Descriptor<AbstractBuildParameterFactory>> getBuilderConfigFactoryDescriptors() {
            return Hudson.getInstance().<AbstractBuildParameterFactory,
              Descriptor<AbstractBuildParameterFactory>>getDescriptorList(AbstractBuildParameterFactory.class);
        }

        @Restricted(DoNotUse.class)
        public boolean isItemGroup(AbstractProject project){
            return project instanceof ItemGroup;
        }

        /**
         * Form validation method.
         *
         * Copied from hudson.tasks.BuildTrigger.doCheck(Item project, String value)
         */
        public FormValidation doCheckProjects(@AncestorInPath Job<?,?> project, @QueryParameter String value ) {
            // JENKINS-32527: Check that it behaves gracefully for an unknown context
            if (project == null) return FormValidation.ok("Context Unknown: the value specified cannot be validated");
            // Require CONFIGURE permission on this project
            if(!project.hasPermission(Item.CONFIGURE)){
            	return FormValidation.ok();
            }
            StringTokenizer tokens = new StringTokenizer(Util.fixNull(value),",");
            boolean hasProjects = false;
            while(tokens.hasMoreTokens()) {
                String projectName = tokens.nextToken().trim();
                if (StringUtils.isBlank(projectName)) {
                    return FormValidation.error("Blank project name in the list");
                }

                Item item = Jenkins.getInstance().getItem(projectName,project,Item.class); // only works after version 1.410
                if(item==null){
                    Item nearest = Items.findNearest(Job.class, projectName, Jenkins.getInstance());
                    String alternative = nearest != null ? nearest.getRelativeNameFrom(project) : "?";
                    return FormValidation.error(Messages.BuildTrigger_NoSuchProject(projectName, alternative));
                }
                if(!(item instanceof Job) || !(item instanceof ParameterizedJobMixIn.ParameterizedJob)) {
                    return FormValidation.error(Messages.BuildTrigger_NotBuildable(projectName));
                }

                // check whether the supposed user is expected to be able to build
                Authentication auth = Tasks.getAuthenticationOf((ParameterizedJobMixIn.ParameterizedJob)project);
                if (auth.equals(ACL.SYSTEM) && !QueueItemAuthenticatorConfiguration.get().getAuthenticators().isEmpty()) {
                    auth = Jenkins.ANONYMOUS;
                }
                if (!item.getACL().hasPermission(auth, Item.BUILD)) {
                    return FormValidation.error(Messages.BuildTrigger_you_have_no_permission_to_build_(projectName));
                }

                hasProjects = true;
            }
            if (!hasProjects) {
            	return FormValidation.error(Messages.BuildTrigger_NoProjectSpecified());
            }

            return FormValidation.ok();
        }

        /**
         * Autocompletion method
         *
         * Copied from hudson.tasks.BuildTrigger.doAutoCompleteChildProjects(String value)
         *
         * @param value
         * @return
         */
        public AutoCompletionCandidates doAutoCompleteProjects(@QueryParameter String value, @AncestorInPath ItemGroup context) {
            AutoCompletionCandidates candidates = new AutoCompletionCandidates();
            List<Job> jobs = Jenkins.getInstance().getAllItems(Job.class);
            for (Job job: jobs) {
                String relativeName = job.getRelativeNameFrom(context);
                if (relativeName.startsWith(value)) {
                    if (job.hasPermission(Item.READ)) {
                        candidates.add(relativeName);
                    }
                }
            }
            return candidates;
        }

    }
}
