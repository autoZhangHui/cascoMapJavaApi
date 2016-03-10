package analysis.hbase.test;
import java.io.IOException;
import java.math.*;
public class analysisMath {
	public double mySum(double[] input){
		int n=input.length;
		int sum=0;
		for(int i=0;i<n;i++){
			sum+=input[i];
		}
		return sum;
	}

	public static double calculateYbyX(double[] averageData){
		double YbyX=0;
		int dataNum=averageData.length;
		double[] monthData=new double[dataNum];
		for(int i=1;i<=dataNum;i++){
			monthData[i-1]=i;
		}
		double sumY=0;
		double sumX=0;
		double sumXY=0;
		double sumXX=0;
		for(int i=0;i<dataNum;i++){
			sumY+=averageData[i];
			sumX+=monthData[i];
			sumXY+=averageData[i]*monthData[i];
			sumXX+=monthData[i]*monthData[i];
		}
		YbyX=(dataNum*sumXY-sumX*sumY)/(dataNum*sumXX-sumX*sumX);
		return YbyX;
	}
	public static String calculateUsual(double[] averageData,String[] month){
		int n=averageData.length;
		double sum=0;
		String result="";
		for(int i=0;i<n;i++){
			sum+=averageData[i];
		}
		sum=sum/n;
		double[] minus=new double[n];
		for(int i=0;i<n;i++){
			minus[i]=averageData[i]-sum;
			minus[i]=Math.abs(minus[i]/sum);
			if(minus[i]>0.1){
				System.out.print(i+"data"+averageData[i]+":"+minus[i]);
				result=month[i]+":";
			}
		}
		return result;
	}
}
