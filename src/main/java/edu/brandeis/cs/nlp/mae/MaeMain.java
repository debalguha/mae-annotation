/*
 * MAE - Multi-purpose Annotation Environment
 *
 * Copyright Keigh Rim (krim@brandeis.edu)
 * Department of Computer Science, Brandeis University
 * Original program by Amber Stubbs (astubbs@cs.brandeis.edu)
 *
 * MAE is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, @see <a href="http://www.gnu.org/licenses">http://www.gnu.org/licenses</a>.
 *
 * For feedback, reporting bugs, use the project on Github
 * @see <a href="https://github.com/keighrim/mae-annotation">https://github.com/keighrim/mae-annotation</a>.
 */

package edu.brandeis.cs.nlp.mae;

import edu.brandeis.cs.nlp.mae.controller.MaeMainController;
import edu.brandeis.cs.nlp.mae.database.MySQLDriverBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.swing.*;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * Created by krim on 4/8/16.
 */
public class MaeMain {

    private static final Logger logger = LoggerFactory.getLogger(MaeMain.class.getName());
    private static Properties applicationProperties = new Properties();
    
    private static void loadApplicationProperties() throws MaeException{
    	try {
    		applicationProperties.load(MySQLDriverBuilder.class.getClassLoader().getResourceAsStream("application.properties"));
		} catch (IOException e) {
			throw new MaeException(e);
		}
    }

    private static void enableOSXQuitStrategy() {
        // for two reasons:
        // 1) unless using apple jdk extensions (com.apple.eawt.Application, QuitStagety)
        // windowClosing() event is not properly fired on OSX, which used for integrity checks and destroying drivers
        // 2) cannot just import such classes and methods, because they exist only on Macs
        // which will cause class-not-found error on other platform (is java really cross-platform?)
        try {
            final Class applicationClass = Class.forName("com.apple.eawt.Application");
            final Method getApplication = applicationClass.getMethod("getApplication");
            final Object applicationObject = getApplication.invoke(applicationClass);

            final Class strategy = Class.forName("com.apple.eawt.QuitStrategy");
            final Enum CLOSE_ALL_WINDOWS = Enum.valueOf(strategy, "CLOSE_ALL_WINDOWS");

            final Method setQuitStrategy = applicationClass.getMethod("setQuitStrategy", strategy);
            setQuitStrategy.invoke(applicationObject, CLOSE_ALL_WINDOWS);

            logger.info("OSX is detected");
        } catch (ClassNotFoundException | NoSuchMethodException |
                SecurityException | IllegalAccessException |
                IllegalArgumentException | InvocationTargetException exp) {
            logger.info("Not on OSX");
        }
    }

    private static MaeMainController createAndShowGUI() {
        enableOSXQuitStrategy();

        MaeMainController controller = new MaeMainController(applicationProperties);
        JFrame mainFrame = controller.initUI();
        controller.setWindowFrame(mainFrame);
        mainFrame.pack();
        mainFrame.setSize(900, 700);
        mainFrame.setVisible(true);

        return controller;

    }

    public static void main(final String[] args) {

        SwingUtilities.invokeLater(new Runnable() {
            @Override
            public void run() {
            	try {
					loadApplicationProperties();
				} catch (MaeException e1) {
					throw new RuntimeException(e1);
				}
                MaeMainController controller = createAndShowGUI();

                if (args.length > 0) {
                    boolean argCmd = false;
                    List<String> argsList = new ArrayList<>();
                    String tFilename = null;
                    String dFilename = null;
                    String dFilenames = null;
                    Collections.addAll(argsList, args);
                    if (argsList.contains("--task")) {
                        tFilename = argsList.get(argsList.indexOf("--task") + 1);
                        argCmd = true;
                        if (argsList.contains("--doc")) {
                            dFilename = argsList.get(argsList.indexOf("--doc") + 1);

                        } else if (argsList.contains("--docs")) {
                            dFilenames = argsList.get(argsList.indexOf("--docs") + 1);

                        }
                    }
                    if (!argCmd) {
                        System.out.println("TODO: show some help text");
                    }

                    if (tFilename != null) {
                        controller.setupScheme(new File(tFilename), true);
                        if (dFilename != null) {
                            controller.addDocument(new File(dFilename));
                        } else if (dFilenames != null) {
                            String[] filesToOpen = dFilenames.split(",");
                            for (String fileName : filesToOpen) {
                                controller.addDocument(new File((fileName)));
                                try {
                                    Thread.sleep(500);
                                } catch (InterruptedException e) {
                                    e.printStackTrace();
                                }
                            }
                        }
                    }
                }
            }
        });
    }
}
