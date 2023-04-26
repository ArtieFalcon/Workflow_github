import unittest
from Unit_test_Home_task import Solution
import ast #для преобразования строки в словарь
#Test cases to test Unit_test_task1 methods
#You always create  a child class derived from unittest.TestCase
class TestUnit_test_task1(unittest.TestCase):
  #setUp method is overridden from the parent class TestCase
    def setUp(self):
        self.solution = Solution()
#Each test method starts with the keyword test_
    def test_check1(self):
        try:
            self.assertTrue(self.solution.check1() == 'Алла' or self.solution.check1() == 'aлла')
        except AttributeError:
            return 'FAILL'
    def test_check2(self):
        try:
            self.assertTrue(self.solution.check2() == ['Алла', 'Александр', 'Вера', 'Елена', 'Роман'] \
            or self.solution.check2() == "['Алла', 'Александр', 'Вера', 'Елена', 'Роман']")
        except AttributeError:
            return 'FAILL'
    def test_isPalindrome(self):
        try:
            self.assertTrue(Solution().isPalindrome(121)) # Истина
            self.assertTrue(Solution().isPalindrome(541207818702145))
            self.assertFalse(Solution().isPalindrome(-121)) # Ложь
            self.assertFalse(Solution().isPalindrome(10))
        except AttributeError:
            return 'FAILL'

# Executing the tests in the above test case class
if __name__ == "__main__":
  unittest.main()