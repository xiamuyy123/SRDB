package com.yq.srdb.backend;

import java.util.HashMap;
import java.util.Map;

public class Test {
    @org.junit.Test
    public void test(){
        Map<Integer,Integer> map = new HashMap<>();
//        map.put(null,1);
        System.out.println(map.getOrDefault(null,2));
        int[][] dp = new int[2][2];
        dp[0][1]=  1;
    }

    @org.junit.Test
    public void main(){
        StringBuilder s = new StringBuilder("");

        solve("0001000100");
        solve2(new int[]{2,1,3,1,3});
    }
    public void solve(String str){
        int len = str.length();
        int[][] dp = new int[len+1][len+1];
        for (int i = 1; i <len; i++) {
            for (int j = i; j < len; j++) {
                if(str.charAt(j)!=str.charAt(j-1)){
                    for (int k = j+1; k < len+1; k++) {
                        dp[i][k] = 1;
                    }
                    break;
                }
            }
        }
        int sum=0;
        for (int i = 1; i < len + 1; i++) {
            for (int j = 1; j < len+1; j++) {
                if(dp[i][j]==1){
                    sum++;
                }
            }
        }
        System.out.println(sum);
    }
    public void solve2(int[] nums){

        int res = 0,sum=0;
        for (int i = 0; i <nums.length; i++) {
            res+=sum*nums[i];
            sum+=nums[i];
        }
        System.out.println(res);;
    }


}
