package edu.neu.hadoop.conf;

import java.io.Serializable;

import edu.neu.hadoop.fs.Path;
import edu.neu.hadoop.mapreduce.Mapper;
import edu.neu.hadoop.mapreduce.Reducer;


/**
 * Provides access to configuration parameters.
 * 
 * @author Adib Alwani
 */
@SuppressWarnings({ "rawtypes", "serial" })
public class Configuration implements Serializable {
	
	private Class<? extends Mapper> mapperClass;
	private Class<? extends Reducer> reducerClass;
	private Class<?> mapOutputKeyClass;
	private Class<?> mapOutputValueClass;
	private Class<?> outputKeyClass;
	private Class<?> outputValueClass;
	private String jar;
	private Class<?> jarByClass;
	private Path[] inputPath;
	private Path outputPath;
	private int numReduceTasks;
	
	public Configuration() {
		numReduceTasks = 1;
	}
	
	public Class<? extends Mapper> getMapperClass() {
		return mapperClass;
	}
	public void setMapperClass(Class<? extends Mapper> mapperClass) {
		this.mapperClass = mapperClass;
	}
	public Class<? extends Reducer> getReducerClass() {
		return reducerClass;
	}
	public void setReducerClass(Class<? extends Reducer> reducerClass) {
		this.reducerClass = reducerClass;
	}
	public Class<?> getMapOutputKeyClass() {
		return mapOutputKeyClass;
	}
	public void setMapOutputKeyClass(Class<?> mapOutputKeyClass) {
		this.mapOutputKeyClass = mapOutputKeyClass;
	}
	public Class<?> getMapOutputValueClass() {
		return mapOutputValueClass;
	}
	public void setMapOutputValueClass(Class<?> mapOutputValueClass) {
		this.mapOutputValueClass = mapOutputValueClass;
	}
	public Class<?> getOutputKeyClass() {
		return outputKeyClass;
	}
	public void setOutputKeyClass(Class<?> outputKeyClass) {
		this.outputKeyClass = outputKeyClass;
	}
	public Class<?> getOutputValueClass() {
		return outputValueClass;
	}
	public void setOutputValueClass(Class<?> outputValueClass) {
		this.outputValueClass = outputValueClass;
	}
	public String getJar() {
		return jar;
	}
	public void setJar(String jar) {
		this.jar = jar;
	}
	public Class<?> getJarByClass() {
		return jarByClass;
	}
	public void setJarByClass(Class<?> jarByClass) {
		this.jarByClass = jarByClass;
	}
	public Path[] getInputPath() {
		return inputPath;
	}
	public void setInputPath(Path[] inputPath) {
		this.inputPath = inputPath;
	}
	public Path getOutputPath() {
		return outputPath;
	}
	public void setOutputPath(Path outputPath) {
		this.outputPath = outputPath;
	}
	public int getNumReduceTasks() {
		return numReduceTasks;
	}
	public void setNumReduceTasks(int numReduceTasks) {
		this.numReduceTasks = numReduceTasks;
	}
}
