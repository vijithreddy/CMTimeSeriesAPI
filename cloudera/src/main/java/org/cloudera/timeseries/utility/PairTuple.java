package org.cloudera.timeseries.utility;

public class PairTuple<T1,T2> {
	private final T1 firstElement;
	private final T2 secondElement;
	
	 /**
	  * Constructor for PairTuple
	 * @param firstElement first element entry
	 * @param secondElement second element entry
	 */
	public PairTuple(T1 firstElement, T2 secondElement) {
		    this.firstElement = firstElement;
		    this.secondElement = secondElement;
		  }

	/**
	 * Getter for first element
	 * @return first element
	 */
	public T1 getFirstElement() {
		return firstElement;
	}

	/**
	 * Getter for second element
	 * @return second element
	 */
	public T2 getSecondElement() {
		return secondElement;
	}

}
