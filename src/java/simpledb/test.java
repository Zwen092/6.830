package simpledb;

import com.sun.source.tree.PackageTree;
import simpledb.common.Database;
import simpledb.common.Type;
import simpledb.execution.SeqScan;
import simpledb.storage.HeapFile;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionId;

import java.io.File;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author zwen
 * @Description
 * @create 2021-11-28 10:52 上午
 */

class resource {
    int num;

    private Lock lock = new ReentrantLock(false);
    private Condition condition1 = lock.newCondition();
    private Condition condition2 = lock.newCondition();

    int flag = 1;


    public void printOdd() throws InterruptedException {
        lock.lock();

        while (flag != 1) {
            condition1.await();

        }
        flag = 2;
        num++;
        System.out.println(Thread.currentThread().getName() + " " + num);
        condition2.signal();
        lock.unlock();

    }

    public void printEven() throws InterruptedException {
        lock.lock();

        while (flag != 2) {
            condition2.await();
        }
        flag = 1;
        num++;
        System.out.println(Thread.currentThread().getName() + " " + num);
        condition1.signal();
        lock.unlock();

    }


}

public class test{
        test[] t = new test[26];



    public static void main(String[] argv) {
        System.out.println(Math.sqrt(12));

    }

    public static int testweightbagproblem(int[] weight, int[] value, int bagsize){

        int[][] dp = new int[weight.length + 1][bagsize + 1];
        //第i件物品，放入j容量大小的背包，获得的最大价值
        for (int i = 1; i <= weight.length; i++) {
            for (int j = 1; j <= bagsize; j++) {
                if (j < weight[i - 1]) {
                    //放不下
                    dp[i][j] = dp[i - 1][j];
                } else {
                    //放得下，放或者不放
                    dp[i][j] = Math.max(dp[i - 1][j], dp[i - 1][j - weight[i - 1]] + value[i - 1]);
                }
            }
        }
        return dp[value.length][bagsize];
    }

    public static int findDuplicate(int[] nums) {
        Set<Integer> s = new HashSet<>();
        for (int i : s) {
            if (!s.add(i)) return i;
        }
        return -1;

    }

    private static void swap(int[][] nums, int i, int j) {
        int[] temp = nums[i];
        nums[i] = nums[j];
        nums[j] = temp;
    }

    public int numIslands(char[][] grid) {
        int count = 0;
        for (int i = 0, n = grid.length; i < n; i++) {
            for (int j = 0, m = grid[0].length; j < m; j++) {
                if (grid[i][j] == '1') {
                    dfs(grid, i, j);

                    count++;

                }
            }
        }


        return count;
    }
    int[] directions = {-1, 0, 1, 0, -1};
    private void dfs(char[][] grid, int x, int y) {
        grid[x][y] = '0';
        for (int i = 0; i < directions.length - 1; i++) {
            int newX = x + directions[i];
            int newY = y + directions[i + 1];
            if (newX >= 0 && newX < grid.length && newY >= 0
                    && newY < grid[0].length && grid[newX][newY] == 1) {
                dfs(grid, newX, newY);
            }
        }
    }

    private static void quickSort(int[] nums, int left, int right) {

        if (left > right) return;
        int position = getPosition(nums, left, right);
        quickSort(nums, 0, position - 1 );
        quickSort(nums, position + 1, right);

    }

    private static int getPosition(int[] nums, int left, int right) {
        int leftPointer = left + 1, rightPointer = right;
        while (true) {
            while (leftPointer <= right && nums[leftPointer] < nums[left]) {
                leftPointer++;
            }
            while (leftPointer <= right && nums[rightPointer] > nums[left]) {
                rightPointer--;
            }
            if (leftPointer > rightPointer) break;
            swap(nums, leftPointer, rightPointer);
            leftPointer++; rightPointer--;
        }
        swap(nums, left, rightPointer);
        return rightPointer;
    }

    private static void swap(int[] nums, int left, int right) {
        int temp = nums[left];
        nums[left] = nums[right];
        nums[right] = temp;
    }

}
