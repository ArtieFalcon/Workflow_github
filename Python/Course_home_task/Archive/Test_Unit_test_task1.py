import unittest
#from Unit_test_task1 import Solution
from Unit_test_Home_task import Solution

#Test cases to test Unit_test_task1 methods
#You always create  a child class derived from unittest.TestCase
class TestUnit_test_task1(unittest.TestCase):
  #setUp method is overridden from the parent class TestCase
    def setUp(self):
        self.solution = Solution()
    
#Each test method starts with the keyword test_
    def test_check1(self):
    #self.assertEqual(self.solution.check1(), 'WorldPeace')
        try:
            self.assertTrue(self.solution.check1() == 'WorldPeace' or self.solution.check1() == 'World Peace')
            #print('try passed')
        except AttributeError:
            #print('not')
            return 'FAILL'
        #print('check1 done')
   
    def test_check2(self):
        try:
            self.assertTrue(self.solution.check2() == ['Роман', 'Елена', 'Вера', 'Александр', 'Алла'] \
            or self.solution.check2() == "['Роман', 'Елена', 'Вера', 'Александр', 'Алла']")
                #return True
            #print('try passed')
        except AttributeError:
            #print('not')
            return 'FAILL'
        #print('check2 done')
    
    def test_isPalindrome(self):
        try:
            self.assertTrue(Solution().isPalindrome(121)) # Истина
            self.assertTrue(Solution().isPalindrome(541207818702145))
            self.assertFalse(Solution().isPalindrome(-121)) # Ложь
            self.assertFalse(Solution().isPalindrome(10))
        except AttributeError:
            return 'FAILL'
#        except TypeError:
#            return 'FAILL'

# Executing the tests in the above test case class
if __name__ == "__main__":
  unittest.main()