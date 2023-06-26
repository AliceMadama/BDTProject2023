from datetime import date
import holidays


class Holiday:
    def __init__(self, country, state):
        self.country = country
        self.state = state
        self.holidays = self.get_holidays()

    def get_holidays(self):
        holiday_list = []
        for year in range(2000, 2023):
            for date, name in holidays.country_holidays(self.country, years=year, prov=self.state).items():
                holiday_list.append(date)
        return holiday_list

    def is_holiday(self, check_date):
        if check_date in self.holidays:
            return True
        return False


# Usage example
holiday = Holiday(country="US", state="NY")

# Check if a specific date is a holiday
check_date = date(2022, 12, 2)  # Example: December 25, 2022
print(holiday.is_holiday(check_date))
