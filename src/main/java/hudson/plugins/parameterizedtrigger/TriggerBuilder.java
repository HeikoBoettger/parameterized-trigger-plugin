/*
 * The MIT License
 *
 * Copyright (c) 2010-2011, InfraDNA, Inc., Manufacture Francaise des Pneumatiques Michelin,
 * Romain Seguy
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

package hudson.plugins.parameterizedtrigger;

import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.servlet.SingleThreadModel;

import org.kohsuke.accmod.Restricted;
import org.kohsuke.stapler.DataBoundConstructor;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ListMultimap;

import hudson.AbortException;
import hudson.EnvVars;
import hudson.Extension;
import hudson.ExtensionList;
import hudson.Launcher;
import hudson.Util;
import hudson.console.ConsoleNote;
import hudson.console.HyperlinkNote;
import hudson.model.AbstractBuild;
import hudson.model.AbstractProject;
import hudson.model.Action;
import hudson.model.BuildListener;
import hudson.model.Cause;
import hudson.model.Job;
import hudson.model.Result;
import hudson.model.Run;
import hudson.model.queue.QueueListener;
import hudson.model.queue.QueueTaskFuture;
import hudson.tasks.BuildStepDescriptor;
import hudson.tasks.BuildStepMonitor;
import hudson.tasks.Builder;
import hudson.triggers.Trigger;
import hudson.util.IOException2;

/**
 * {@link Builder} that triggers other projects and optionally waits for their
 * completion.
 *
 * @author Kohsuke Kawaguchi
 */
public class TriggerBuilder extends Builder {

  private static final Logger LOGGER = Logger.getLogger(TriggerBuilder.class.getName());

  private final ArrayList<BlockableBuildTriggerConfig> configs;

  @DataBoundConstructor
  public TriggerBuilder(List<BlockableBuildTriggerConfig> configs) {
    this.configs = new ArrayList<BlockableBuildTriggerConfig>(Util.fixNull(configs));
  }

  public TriggerBuilder(BlockableBuildTriggerConfig... configs) {
    this(Arrays.asList(configs));
  }

  public List<BlockableBuildTriggerConfig> getConfigs() {
    return configs;
  }

  @Override
  public BuildStepMonitor getRequiredMonitorService() {
    return BuildStepMonitor.NONE;
  }

  @Override
  public boolean perform(final AbstractBuild<?, ?> build, Launcher launcher, final BuildListener listener)
      throws InterruptedException, IOException {
    EnvVars env = build.getEnvironment(listener);
    env.overrideAll(build.getBuildVariables());

    AtomicBoolean buildStepResult = new AtomicBoolean(true);

    Map<Object, String> jobHyperlinks = new HashMap<Object, String>();
    try {
      for (final BlockableBuildTriggerConfig config : configs) {
        ListMultimap<Job, QueueTaskFuture<? extends AbstractBuild>> futures = config.perform3(build, launcher, listener);
        // Only contains resolved projects
        List<Job> projectList = config.getJobs(build.getRootBuild().getProject().getParent(), env);

        // Get the actual defined projects
        StringTokenizer tokenizer = new StringTokenizer(config.getProjects(env), ",");

        if (tokenizer.countTokens() == 0) {
          throw new AbortException("Build aborted. No projects to trigger. Check your configuration!");
        } else if (tokenizer.countTokens() != projectList.size()) {

          int nbrOfResolved = tokenizer.countTokens() - projectList.size();

          // Identify the unresolved project(s)
          Set<String> unsolvedProjectNames = new TreeSet<String>();
          while (tokenizer.hasMoreTokens()) {
            unsolvedProjectNames.add(tokenizer.nextToken().trim());
          }
          for (Job project : projectList) {
            unsolvedProjectNames.remove(project.getFullName());
          }

          // Present the undefined project(s) in error message
          StringBuffer missingProject = new StringBuffer();
          for (String projectName : unsolvedProjectNames) {
            missingProject.append(" > ");
            missingProject.append(projectName);
            missingProject.append("\n");
          }

          throw new AbortException("Build aborted. Can't trigger undefined projects. " + nbrOfResolved
              + " of the below project(s) can't be resolved:\n" + missingProject.toString()
              + "Check your configuration!");
        } else {
          // handle non-blocking configs
          if (futures.isEmpty()) {
            listener.getLogger().println("Triggering projects: " + getProjectListAsString(jobHyperlinks, projectList));
            for (Job p : projectList) {
              BuildInfoExporterAction.addBuildInfoExporterAction(build, p.getFullName());
            }
            continue;
          }

          // handle blocking configs
          for (Job p : projectList) {
            // handle non-buildable projects
            if (!p.isBuildable()) {
              listener.getLogger().println("Skipping " + getOrCreateHyperlinkNote(jobHyperlinks, p)
                  + ". The project is either disabled or the configuration has not been saved yet.");
              continue;
            }
            for (QueueTaskFuture<? extends AbstractBuild> future : futures.get(p)) {
              listener.getLogger()
                  .println("Waiting for the completion of " + getOrCreateHyperlinkNote(jobHyperlinks, p));
              BuildInfoExporterAction.addBuildInfoExporterAction(build, future);
            }
          }

          final Semaphore trigger = new Semaphore(0);
          Set<Future<? extends AbstractBuild>> done = new HashSet<Future<? extends AbstractBuild>>();
          boolean allDone = loopJobs(config, build, projectList, futures, done, jobHyperlinks, listener, buildStepResult, new PendingJobCallback() {
              
              @Override
              public void pending(final QueueTaskFuture<? extends AbstractBuild> future) {
                //there is no other way than scheduling a thread an waiting for the future to complete
                //BuildListener is not triggered at all (only the logger is used) and queuelisteners are
                //not synchronized with the state of the future resulting in deadlocks
                new Thread(new Runnable() {
                  
                  @Override
                  public void run() {
                    try {
                      future.get();
                    } catch (InterruptedException e) {
                    } catch (ExecutionException e) {
                    }
                    trigger.release();
                  }
                }).start();
              }
            });
    
          while (!allDone) {
            trigger.acquire(1);
            allDone = loopJobs(config, build, projectList, futures, done, jobHyperlinks, listener, buildStepResult, new PendingJobCallback() {

              @Override
              public void pending(QueueTaskFuture<? extends AbstractBuild> future) {
                //nothing do here, we already scheduled all tasks
              }

            });
          } 
          BuildInfoExporterAction action = build.getAction(BuildInfoExporterAction.class);
          if (action != null)
            action.updateReferences();
        }
      }
    } catch (ExecutionException e) {
      throw new IOException2(e); // can't happen, I think.
    }
    return buildStepResult.get();
  }
  
  public static interface PendingJobCallback {

    public void pending(QueueTaskFuture<? extends AbstractBuild> future);
    
  }

  private boolean loopJobs(BlockableBuildTriggerConfig config, AbstractBuild<?, ?> build, List<Job> projectList, ListMultimap<Job, QueueTaskFuture<? extends AbstractBuild>> futures, Set<Future<? extends AbstractBuild>> done, Map<Object, String> jobHyperlinks, BuildListener listener, AtomicBoolean buildStepResult, PendingJobCallback callback) throws AbortException, InterruptedException, ExecutionException {
    boolean allDone = true;
    for (Job<?, ?> p : projectList) {
      // handle non-buildable projects
      if (!p.isBuildable()) {
        continue;
      }

      for (QueueTaskFuture<? extends AbstractBuild> future : futures.get(p)) {
        try {
                                if (future != null ) {
                                    listener.getLogger().println("Checking for the completion of "
                                        + getOrCreateHyperlinkNote(jobHyperlinks, p));
          Future<? extends AbstractBuild> startCondition = future.getStartCondition();
          if (!startCondition.isDone()) {
            allDone = false;
            callback.pending(future);
            continue;
          }
          if (done.add(startCondition)) {
            listener.getLogger().println(getOrCreateHyperlinkNote(jobHyperlinks, p) + " scheduled.");
          }
          if (!future.isDone()) {
            allDone = false;
            callback.pending(future);
            continue;
          }
          Run b = future.get();
          listener.getLogger().println(
              getOrCreateHyperlinkNote(jobHyperlinks, b) + " completed. Result was " + b.getResult());

          if (buildStepResult.get() && config.getBlock().mapBuildStepResult(b.getResult())) {
            build.setResult(config.getBlock().mapBuildResult(b.getResult()));
          } else {
            buildStepResult.set(false);
                                    }
                                } else {
                                    listener.getLogger().println("Skipping " + HyperlinkNote.encodeTo('/'+ p.getUrl(), p.getFullDisplayName()) + ". The project was not triggered by some reason.");
          }
        } catch (CancellationException x) {
          throw new AbortException(p.getFullDisplayName() + " aborted.");
        }
      }
    }
    return allDone;
  }

  private static String getOrCreateHyperlinkNote(Map<Object, String> hyperlinkNotes, Run run) {
    String note = hyperlinkNotes.get(run);
    if (note == null) {
      /**
       * prevent calling encodeTo to often, it uses gzip deflate which takes
       * long to init
       */
      note = HyperlinkNote.encodeTo('/' + run.getUrl(), run.getFullDisplayName());
      hyperlinkNotes.put(run, note);
    }
    return note;
  }

  private static String getOrCreateHyperlinkNote(Map<Object, String> hyperlinkNotes, Job job) {
    String note = hyperlinkNotes.get(job);
    if (note == null) {
      /**
       * prevent calling encodeTo to often, it uses gzip deflate which takes
       * long to init
       */
      note = HyperlinkNote.encodeTo('/' + job.getUrl(), job.getFullDisplayName());
      hyperlinkNotes.put(job, note);
    }
    return note;
  }

  // Public but restricted so we can add tests without completely changing the
  // tests package
  @Restricted(value = org.kohsuke.accmod.restrictions.NoExternalUse.class)
  public String getProjectListAsString(List<Job> projectList) {
    return getProjectListAsString(new HashMap<Object, String>(), projectList);
  }

  // Public but restricted so we can add tests without completely changing the
  // tests package
 @Restricted(value=org.kohsuke.accmod.restrictions.NoExternalUse.class)
  public String getProjectListAsString(Map<Object, String> hyperlinkNotes, List<Job> projectList) {
    StringBuilder projectListString = new StringBuilder();
    for (Iterator<Job> iterator = projectList.iterator(); iterator.hasNext();) {
      Job project = iterator.next();
      projectListString.append(getOrCreateHyperlinkNote(hyperlinkNotes, project));
      if (iterator.hasNext()) {
        projectListString.append(", ");
      }
    }
    return projectListString.toString();
  }

  @Override
  public Collection<? extends Action> getProjectActions(AbstractProject<?, ?> project) {
    return ImmutableList.of(new SubProjectsAction(project, configs));
  }

  @Extension
  public static class DescriptorImpl extends BuildStepDescriptor<Builder> {

    @Override
    public String getDisplayName() {
      return "Trigger/call builds on other projects";
    }

    @Override
    public boolean isApplicable(Class<? extends AbstractProject> jobType) {
      return true;
    }
  }
}
